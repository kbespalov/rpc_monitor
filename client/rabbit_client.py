import json
import logging
import uuid
from threading import Thread
import abc
import pika
import requests
from oslo_config import cfg

LOG = logging.getLogger('RPC State Controller')
LOG.setLevel(logging.INFO)
LOG.addHandler(logging.StreamHandler())


class FetchingException(Exception):
    pass


class RabbitAPIClient(object):
    # RabbitMQ API resources
    QUEUES = "queues"
    EXCHANGES = "exchanges"
    QUEUE_INFO = "queues/%(vhost)s/%(queue)s"
    Q_BINDINGS = "queues/%(vhost)s/%(queue)s/bindings"
    E_BINDINGS = "exchanges/%(vhost)s/%(exchange)s/bindings/%(type)s"

    # Setup configuration options
    opt_group = cfg.OptGroup(name='rabbit_monitor')
    opts = [cfg.StrOpt('management_url', default='http://localhost:15672'),
            cfg.StrOpt('management_user', default='guest'),
            cfg.StrOpt('management_pass', default='guest')]

    config = cfg.CONF
    config.register_group(opt_group)
    config.register_opts(opts, group=opt_group)

    def __init__(self):
        config = self.get_cfg()
        self.auth = (config.management_user, config.management_pass)
        self.api = config.management_url + "/api/%s/"

    @classmethod
    def get_cfg(cls):
        return cls.config.rabbit_monitor

    def _get(self, resource, data=None):
        r = requests.get(self.api % resource, data, auth=self.auth)
        if r.status_code == 200:
            return r.json()
        else:
            raise FetchingException("Failed to get %s list: response code "
                                    "%s" % (resource, r.status_code))

    def queue_bindings_list(self, queue, vhost='%2F'):
        params = {'vhost': vhost, 'queue': queue}
        return self._get(self.Q_BINDINGS % params)

    def exchange_bindings_list(self, exchange, btype='source', vhost='%2F'):
        params = {'vhost': vhost, 'exchange': exchange, 'type': btype}
        return self._get(self.E_BINDINGS % params)

    def queue_info(self, queue, vhost='%2F'):
        params = {'vhost': vhost, 'queue': queue}
        return self._get(self.QUEUE_INFO % params)

    def queues_list(self, columns='name,consumers'):
        return self._get(self.QUEUES, data=dict(columns=columns))

    def exchanges_list(self, columns='name,type'):
        return self._get(self.EXCHANGES, data=dict(columns=columns))


class AMQPClient(object):
    # Setup configuration options
    opt_group = cfg.OptGroup('rabbit_monitor')
    opts = [cfg.StrOpt('amqp_url', default='amqp://guest:guest@localhost'),
            cfg.StrOpt('reply_to', default='openstack_rpc_reply')]
    config = cfg.CONF
    config.register_group(opt_group)
    config.register_opts(opts, group=opt_group)

    def __init__(self, on_incoming):
        config = self.get_cfg()
        self.params = pika.URLParameters(config.amqp_url)
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.reply_listener = AMQPConsumer(config.amqp_url,
                                           on_incoming,
                                           config.reply_to)
        self.reply_listener.start()

    @classmethod
    def get_cfg(cls):
        return cls.config.rabbit_monitor

    def _publish(self, msg, exchange='', routing_key='*', reply_queue=None,
                 headers=None):
        # the kombu driver getting a reply queue from  a msg payload
        # conversely the pika driver use message properties
        # to store the queue
        properties = pika.BasicProperties(content_type='application/json',
                                          reply_to=reply_queue,
                                          headers=headers)

        self.channel.basic_publish(exchange=exchange,
                                   routing_key=routing_key,
                                   body=json.dumps(msg),
                                   properties=properties)


class AMQPConsumer(object):
    def __init__(self, amqp_url, on_incoming, queue_name):
        self.params = pika.URLParameters(amqp_url)
        self.connection = pika.BlockingConnection(parameters=self.params)
        self.channel = self.connection.channel()
        self.queue = queue_name
        self.exchange_bindings = []
        self.on_incoming = on_incoming
        self._setup_reply_queue()
        self.consumer_thread = Thread(target=self._consume)

    def _setup_reply_queue(self):
        LOG.info("[AMQP Consumer] Setup reply queue: %s" % self.queue)
        self.channel.queue_declare(self.queue)
        self.channel.exchange_declare(self.queue, 'direct',
                                      durable=False,
                                      auto_delete=True)
        self.channel.queue_bind(self.queue, self.queue, self.queue)

    def _consume(self):
        LOG.info("[AMQP Consumer] Starting consuming rpc states ...")
        self.channel.basic_consume(consumer_callback=self._on_message,
                                   no_ack=True,
                                   queue=self.queue)
        self.channel.start_consuming()

    def _on_message(self, ch, method_frame, header_frame, body):
        if body:
            self.on_incoming(body)

    def start(self):
        self.consumer_thread.start()

    def stop(self):
        self.channel.close()
        self.connection.close()


class KombuStateClient(AMQPClient):
    # uses for fanout+reply pattern across all rpc servers
    BROADCAST_EXCHANGE = "rpc_state_broadcast"

    def __init__(self, on_incoming):
        super(KombuStateClient, self).__init__(self.on_kombu_incoming)
        self.on_incoming = on_incoming
        self.rabbit_client = RabbitAPIClient()
        self.bindings = set()
        self._setup_publish_exchange()

    def _setup_publish_exchange(self):
        """The client initialization """
        LOG.info("[kombu] Declare publishing exchange %s ..." % self.BROADCAST_EXCHANGE)
        self.channel.exchange_declare(exchange=self.BROADCAST_EXCHANGE,
                                      type='fanout',
                                      durable=False)
        exchanges = self.rabbit_client.exchanges_list()
        self.setup_exchange_bindings(exchanges)

    def _is_related_exchange(self, exchange):
        """Check if the exchange is fanout exchange of rpc server of kombu driver"""
        if exchange['type'] == 'fanout':
            if '_fanout' in exchange['name']:
                if exchange['name'] not in self.bindings:
                    return True
        return False

    def on_kombu_incoming(self, incoming):
        body = json.loads(incoming)
        if 'oslo.message' in body:
            message = body['oslo.message']
            message = json.loads(message)
            if 'result' in message:
                return self.on_incoming(message['result'])
        LOG.error('[kombu] Failed to process incoming message: %s' % incoming)

    def setup_exchange_bindings(self, exchanges):
        """Bind fanout exchanges of rpc servers to the BROADCAST_EXCHANGE """
        LOG.info("Setup bindings to broadcast exchange ...")
        for exchange in filter(self._is_related_exchange, exchanges):
            LOG.info("[kombu] Successful binding: %s -> %s " % (self.BROADCAST_EXCHANGE, exchange['name']))
            self.channel.exchange_bind(source=self.BROADCAST_EXCHANGE,
                                       destination=exchange['name'],
                                       routing_key='rpc_state')
            self.bindings.add(exchange['name'])

    def _create_call_msg(self, method, args=None):
        """ Create rpc.call message according kombu driver format """
        msg_id = uuid.uuid4().hex
        msg = dict()
        msg['method'] = method
        msg['_unique_id'] = msg_id
        msg['_msg_id'] = msg_id
        msg['_reply_q'] = self.REPLY_QUEUE
        msg['version'] = '1.0'
        msg['args'] = args or {}
        return msg

    def ping(self, request_time, exchange=BROADCAST_EXCHANGE, routing_key=""):
        args = {'request_time': request_time}
        msg = self._create_call_msg('echo_reply', args)
        self._publish(msg, exchange, routing_key)

    def get_rpc_stats(self, request_time, exchange=BROADCAST_EXCHANGE, routing_key=""):
        args = {'request_time': request_time}
        msg = self._create_call_msg('rpc_stats', args)
        self._publish(msg, exchange, routing_key)


class PikaStateClient(AMQPClient):
    # all queues of rpc servers are bind to the exchange
    PIKA_MAIN_EXCHANGE = "openstack_rpc"
    BROADCAST_EXCHANGE = "rpc_state_broadcast"

    def __init__(self, on_incoming):
        super(PikaStateClient, self).__init__(self.on_pika_incoming)
        self.on_incoming = on_incoming
        self.rabbit_client = RabbitAPIClient()
        self.fanout_routing_keys = set()
        self.update_routing_keys()

    def _is_related_binding(self, binding):
        if binding['routing_key'].endswith("all_workers"):
            if binding['routing_key'].startswith("with_ack"):
                return True
        return False

    def on_pika_incoming(self, incoming):
        body = json.loads(incoming)
        if 's' in body:
            return self.on_incoming(body['s'])
        LOG.error('[pika] Failed to process incoming message: %s' % incoming)

    def update_routing_keys(self):
        LOG.info('[pika] Updating list of fanout routing keys')
        bindings = self.rabbit_client.exchange_bindings_list(self.PIKA_MAIN_EXCHANGE)
        for bind in filter(self._is_related_binding, bindings):
            key = bind['routing_key']
            LOG.info(' -- %s ' % key)
            self.fanout_routing_keys.add(key)

    def _create_call_msg(self, method, args=None):
        msg = dict()
        msg['method'] = method
        msg['args'] = args or {}
        return msg

    def ping(self, request_time, exchange=PIKA_MAIN_EXCHANGE, routing_key=None):
        args = {'request_time': request_time}
        msg = self._create_call_msg('echo_reply', args)

        if not routing_key:
            for key in self.fanout_routing_keys:
                self._publish(msg, exchange, key, self.REPLY_QUEUE, {'version': '1.0'})
        else:
            self._publish(msg, exchange, routing_key, self.REPLY_QUEUE, {'version': '1.0'})

    def get_rpc_stats(self, request_time, exchange=PIKA_MAIN_EXCHANGE, routing_key=None):
        args = {'request_time': request_time}
        msg = self._create_call_msg('rpc_stats', args)

        if not routing_key:
            for key in self.fanout_routing_keys:
                self._publish(msg, exchange, key, self.REPLY_QUEUE, {'version': '1.0'})
        else:
            self._publish(msg, exchange, routing_key, self.REPLY_QUEUE, {'version': '1.0'})

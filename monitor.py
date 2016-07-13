import Queue
import logging

import time
from threading import Thread

from concurrent.futures import ThreadPoolExecutor
from oslo_config import cfg
from client.rabbit_client import RPCStateClient
from state.influx_repository import InfluxDBStateRepository

LOG = logging.Logger(__name__)


class RPCStateMonitor(object):
    def __init__(self):
        config = self.setup_config()
        # repository for storing and processing incoming state samples
        self.repository = InfluxDBStateRepository(config.poll_delay)
        self.callbacks_routes = {'sample': self.repository.on_incoming}
        # setup workers for processing incoming states
        self.incoming = Queue.Queue()
        self.client = RPCStateClient(self.on_incoming)
        self.workers = ThreadPoolExecutor(config.workers)
        for _ in xrange(config.workers):
            self.workers.submit(self.worker)
        # setup periodic poller with specified delay

        self.poll_delay = config.poll_delay
        if not config.listener_only:
            self.periodic_updates = Thread(target=self.update_rpc_state)
            self.periodic_updates.start()

    def worker(self):
        while True:
            resp_time, response = self.incoming.get()
            msg_type = response['msg_type']
            self.callbacks_routes[msg_type](resp_time, response)

    @staticmethod
    def setup_config():
        opts = [cfg.IntOpt(name='poll_delay', default=60),
                cfg.IntOpt(name='workers', default=30),
                cfg.BoolOpt('listener_only', default=False)]
        config = cfg.CONF
        config.register_opts(opts)
        return config

    def on_incoming(self, msg):
        response = msg['result']
        if response:
            self.incoming.put((time.time(), response))
        else:
            print 'Failed process message: %s ' % str(msg)

    def update_exchanges_list(self):
        exchanges = self.client.rabbit_client.exchanges_list()
        self.client.setup_exchange_bindings(exchanges)

    def update_rpc_state(self):
        while True:
            request_time = time.time()
            self.client.get_rpc_stats(request_time)
            time.sleep(self.poll_delay)
            self.update_exchanges_list()

from Queue import Queue, Empty
from threading import Thread
import time
from influx_repository import InfluxDBClient
from oslo_config import cfg

from oslo_messaging.rpc.server import loop_bucket


class InfluxDBReporter(object):

    opts = [cfg.StrOpt('influx_host', default='localhost'),
            cfg.IntOpt('influx_port', default=8086),
            cfg.StrOpt('influx_user', default='root'),
            cfg.StrOpt('influx_pass', default='root'),
            cfg.StrOpt('influx_db', default='rpc_monitor')]

    def __init__(self):
        self.conf = self._setup_conf(cfg.CONF)
        self.samples_queue = Queue(maxsize=1000)

        self.influx_client = InfluxDBClient(self.conf.influx_host,
                                            self.conf.influx_port,
                                            self.conf.influx_user,
                                            self.conf.influx_pass,
                                            self.conf.influx_db)

        self.influx_client.create_database(self.conf.influx_db)
        self.sender = Thread(target=self._sample_sender)
        self.sender.start()

    def _setup_conf(self, conf):
        opt_group = cfg.OptGroup(name='influx_repository')
        conf.register_group(opt_group)
        conf.register_opts(InfluxDBReporter.opts, group=opt_group)
        return conf.influx_repository

    def _populate_batch(self, batch, max_size=1000):
        try:
            timestamp = time.time()
            while 1:
                batch.append(self.samples_queue.get(timeout=5))
                max_size -= 1
                if max_size == 0 or time.time() - timestamp > 5:
                    break
        except Empty:
            pass

    def _sample_sender(self):
        batch = []
        while 1:
            batch.append(self.samples_queue.get())
            self._populate_batch(batch)
            self.influx_client.write_points(batch)
            batch = []


class InfluxDBStateRepository(InfluxDBReporter):
    def __init__(self, update_time):
        super(InfluxDBStateRepository, self).__init__()
        self.worker_timeout = max(update_time, 120)  # (s)
        self.new_workers, self.die_workers = self._workers_events()

    @staticmethod
    def over_methods(sample):
        for endpoint, methods in sample['endpoints'].iteritems():
            for method, state in methods.iteritems():
                yield endpoint, method, state

    def _as_id(self, tags):
        return '|'.join([tags['process_name'], tags['host'], tags['wid']])

    def _workers_events(self):

        events_q = 'select * from workers_state where time > now() - 1h'
        result = self.influx_client.query(events_q).get_points(measurement='workers_state')
        new, die = set(), set()
        for sample in list(result):
            if sample['value'] == 'die':
                die.add(self._as_id(sample))
            else:
                new.add(self._as_id(sample))
        return new, die

    def report_workers_state(self):
        # needs to detect new workers
        q = 'select count(latency) from response_time WHERE %s GROUP BY wid, host, process_name'
        # needs to detect die workers
        l = 'select last(response),wid,host,process_name ' \
            'from response_time Where %s GROUP BY wid, host, process_name'

        def detect_die():
            _latest = self.influx_client.query(l % 'time > now() - 1h')
            die = set()
            now = time.time()
            for sample in _latest.get_points(measurement='response_time'):
                if now - sample['last'] > self.worker_timeout:
                    _id = self._as_id(sample)
                    if _id not in self.die_workers:
                        die.add(_id)
            return die

        def detect_new():
            _ten = self.influx_client.query(q % 'time > now() - 10m')
            _hour = self.influx_client.query(q % 'now() - 1h < time < now() - 10m')
            actual_workers = set(self._as_id(x[1]) for x in _ten.keys())
            history_workers = set(self._as_id(x[1]) for x in _hour.keys())
            new = (actual_workers - history_workers) - self.new_workers
            return new

        def report_workers(workers, state):
            for worker in workers:
                proc, host, wid = worker.split('|')
                self.samples_queue.put({
                    'measurement': 'workers_state',
                    'time': int(time.time() * 10 ** 9),
                    'tags': {
                        'host': host,
                        'process_name': proc,
                        'wid': wid
                    }, 'fields': {
                        'value': state
                    }
                })

        die_workers = detect_die()
        new_workers = detect_new()

        self.new_workers |= new_workers
        self.die_workers |= die_workers

        report_workers(new_workers, 'new')
        report_workers(die_workers, 'die')

    def report_response_time(self, msg):
        response_time = time.time()
        request_time = msg['req_time']
        self.samples_queue.put({
            'measurement': 'response_time',
            'time': int(response_time * 10 ** 9),
            'tags': {
                'host': msg['hostname'],
                'wid': msg['wid'],
                'process_name': msg['proc_name'],
            },
            'fields': {
                'latency': (response_time - request_time),
                'response': response_time
            }
        })

    def report_rpc_stats(self, msg):
        for endpoint, method, state in self.over_methods(msg):
            aligned_time = int(state['latest_call'] - state['latest_call'] % msg['granularity'])
            for bucket in reversed(state['distribution']):
                method_sample = {
                    'measurement': 'rpc_method',
                    'time': aligned_time * 10 ** 9,
                    'tags': {
                        'topic': msg['topic'],
                        'server': msg['server'],
                        'host': msg['hostname'],
                        'wid': msg['wid'],
                        'process_name': msg['proc_name'],
                        'endpoint': endpoint,
                        'method': method
                    },
                    'fields': {
                        'avg': float(loop_bucket.get_avg(bucket)),
                        'max': float(loop_bucket.get_max(bucket)),
                        'min': float(loop_bucket.get_min(bucket)),
                        'cnt': float(loop_bucket.get_cnt(bucket))
                    }
                }
                self.samples_queue.put(method_sample)
                aligned_time -= msg['granularity']

    def append(self, sample):
        self.report_response_time(sample)
        self.report_rpc_stats(sample)
        self.report_workers_state()

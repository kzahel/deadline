import tornado.ioloop
import time
ioloop = tornado.ioloop.IOLoop.instance()
#import tornado.options
#tornado.options.parse_command_line()
import tornado.httpclient
import logging
import json
httpclient = tornado.httpclient.AsyncHTTPClient()

from deadline.util import encode_multipart_formdata

FLUSH_EVENTS = 5


class Manager(object):
    def __init__(self, host):
        self._host = host
        self._stats = []
        self._listeners = []
        self._last_flush = None
        self._first_event_time = None

    def tick(self, t):
        if not self._first_event_time:
            self._first_event_time = t
        if self._last_flush:
            if t - self._last_flush > FLUSH_EVENTS:
                self.flush(t)
        else:
            if t - self._first_event_time > FLUSH_EVENTS:
                self.flush(t)

    def flush(self,t):
        d = {}
        for stat in self._stats:
            d[stat.name] = (stat.meta(), stat.consume())
        self._last_flush = t
        content_type, body = encode_multipart_formdata( (k,json.dumps(v)) for k,v in d.items() )

        req = tornado.httpclient.HTTPRequest('%s/stats' % self._host,
                                             method = 'POST',
                                             log_request = False,
                                             headers = { 'Content-Type': content_type },
                                             body = body)
        #logging.info('flushing %s' % body)
        httpclient.fetch(req, self.flushed)

    def process(self, key, data):
        meta, values = data
        logging.info('processing data for %s, %s' % (meta, values))
        
        if key in self._listeners:
            closed_listeners = []
            for callback in self._listeners[key]:
                retval = callback(values)
                if retval:
                    closed_listeners.append(callback)
            for todelete in closed_listeners:
                self._listeners[key].remove(closed_listeners)

    def flushed(self, response):
        if response.error:
            logging.error('got flush response %s' % response)

    def register(self, stat):
        self._stats.append(stat)

    def add_listener(self, key, callback):
        if key not in self._listeners:
            self._listeners[key] = []
        self._listeners[key].append(callback)

manager = Manager('http://127.0.0.1:8006')

class Gauge(object):
    def __init__(self, name, poll_interval, poll_fn):
        self._values = []
        self.name = name
        self.poll_interval = poll_interval * 1000
        self.poll_fn = poll_fn
        self.periodic = tornado.ioloop.PeriodicCallback( self.poll, self.poll_interval )
        self.periodic.start()
        manager.register(self)

    def meta(self):
        return 'Gauge'

    def poll(self):
        value = self.poll_fn()
        #logging.info('polling for stat %s, got value %s' % (self, value))
        self.add_value(value)

    def add_value(self, v):
        t = time.time()
        self._values.append( (t,v) )
        manager.tick(t)

    def consume(self):
        v = self._values
        self._values = []
        return v


    

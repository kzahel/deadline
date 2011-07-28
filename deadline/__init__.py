import tornado.ioloop
import time
ioloop = tornado.ioloop.IOLoop.instance()
from tornado.options import options, parse_command_line
#parse_command_line()
import tornado.httpclient
import logging
import json
httpclient = tornado.httpclient.AsyncHTTPClient()

from deadline.util import encode_multipart_formdata

FLUSH_EVENTS = 1

FORCE_TICK = 20

DEFAULT_COUNT_WINDOW = 1

class Manager(object):
    def log(self, msg):
        logging.info('deadline manager: %s' % msg)

    def __init__(self, host):
        self._host = host
        self._stats = []
        self._listeners = {}
        self._last_flush = None
        self._first_event_time = None
        self.periodic = tornado.ioloop.PeriodicCallback( self.tick, FORCE_TICK * 1000 )
        self.periodic.start()

    def tick(self, t=None):
        if t is None: t = time.time()
        if not self._first_event_time:
            self._first_event_time = t
        if self._last_flush:
            if t - self._last_flush > FLUSH_EVENTS:
                self.flush(t)
        else:
            if t - self._first_event_time > FLUSH_EVENTS:
                self.flush(t)

    def flush(self,t):
        if not self._host:
            logging.info('not flushing events -- no master host')
            return
        #logging.info('flushing events!')
        d = {}
        for stat in self._stats:
            if stat.ready_for_consume(t):
                d[stat.name] = (stat.meta(), stat.consume(t))
        self._last_flush = t
        content_type, body = encode_multipart_formdata( (k,json.dumps(v)) for k,v in d.items() )
        
        req = tornado.httpclient.HTTPRequest('%s/stats' % self._host,
                                             method = 'POST',
                                             log_request = False,
                                             headers = { 'Content-Type': content_type, 'Source':'%s:%s' % (options.attach_hostname, options.frontend_port) },
                                             body = body)
        #logging.info('flushing %s' % body)
        httpclient.fetch(req, self.flushed)

    def process(self, key, data, opts = None):
        # called by the handler that receives the data
        meta, values = data
        if options.verbose > 0:
            self.log('processing data for %s, %s, %s (with opts: %s)' % (key, meta, values, opts))


        # XXX!!! aggregate across sources!

        
        if key in self._listeners:
            closed_listeners = []
            for listener in self._listeners[key]:
                retval = listener.on_new_data(values)
                if retval:
                    closed_listeners.append(listener)
            for todelete in closed_listeners:
                self._listeners[key].remove(closed_listeners)

    def flushed(self, response):
        if response.error:
            self.log('got flush response %s' % response)

    def register(self, stat):
        self._stats.append(stat)

    def add_listener(self, listener):
        key = listener.key
        if key not in self._listeners:
            self._listeners[key] = []
        self._listeners[key].append(listener)
        self.log('added listener %s' % listener)

    def remove_listener(self, listener):
        key = listener.key
        if key in self._listeners:
            if listener in self._listeners[key]:
                self._listeners[key].remove(listener)
                self.log('removed listener %s' % listener)

if 'deadline_master' in options:
    manager = Manager(options.deadline_master)
else:
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

    def ready_for_consume(self,t):
        if self._values:
            return True

    def consume(self,t=None):
        v = self._values
        self._values = []
        return v


    
class Count(object):
    def __init__(self, name, max_window = DEFAULT_COUNT_WINDOW, max_value=None):
        self.max_window = max_window
        self.max_value = max_value
        self.name = name

        self.windows = []

        self.current_window_begin = time.time()
        self.counter = 0
        manager.register(self)

    def meta(self):
        return 'Count'

    def ready_for_consume(self, t=None):
        if t is None: t = time.time()
        return len(self.windows) > 0 or t - self.current_window_begin > self.max_window or (self.max_value and self.counter >= self.max_value)

    def consume(self,t=None):
        if t is None: t = time.time()
        # return the time window and the count inside the window
        #toreturn = [ (self.current_window_begin, t), self.counter ]
        self.try_consume_window(0, t)
        toreturn = self.windows

        # reset the values for a new count interval
        #self.current_window_begin = t
        #self.counter = 0
        self.windows = []

        return toreturn

    def try_consume_window(self, val=0, t=None):
        if t is None: t=time.time()

        if t - self.current_window_begin > self.max_window:
            # need to make a new window
            window = [ (self.current_window_begin, t), self.counter ]
            self.windows.append( window )
            self.counter = 0
            self.current_window_begin = t

        self.counter += val
        
    def increment(self, val=1):
        t = time.time()
        self.try_consume_window(val,t)

        #if self.ready_for_consume(t):
        #    manager.tick(t)
            
        

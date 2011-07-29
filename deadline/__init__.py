import tornado.ioloop
import time
ioloop = tornado.ioloop.IOLoop.instance()
from tornado.options import options, parse_command_line
#parse_command_line()
import tornado.httpclient
import logging
import json
httpclient = tornado.httpclient.AsyncHTTPClient()
if options.debug:
    import pdb
from deadline.util import encode_multipart_formdata

FLUSH_EVENTS = 1

FORCE_TICK = 3

DEFAULT_COUNT_WINDOW = 1

DEADLINE = 2 # number of windows behind for aggregation

class Manager(object):
    def log(self, msg):
        logging.info('manager: %s' % msg)

    def __init__(self, host):
        if 'service_identifier' in options:
            self._source = '%s' % options.service_identifier
        else:
            self._source = '%s:%s' % (options.attach_hostname, options.frontend_port)
        
        self._host = host
        self._stats = {}
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
            #logging.info('not flushing events -- no master host')
            return
        #logging.info('flushing events!')
        d = {}
        for key, stat in self._stats.items():
            if stat.ready_for_consume(t) and stat.active:
                d[stat.name] = (stat.meta(), stat.consume(t, upstream_flush=True)) # stat gets consumed!!!
        self._last_flush = t
        content_type, body = encode_multipart_formdata( [(k,json.dumps(v)) for k,v in d.items()] )
        headers = {'Source':self._source }
        if content_type:
            headers['Content-Type'] = content_type
        req = tornado.httpclient.HTTPRequest('%s/stats' % self._host,
                                             method = 'POST',
                                             log_request = False,
                                             headers = headers,
                                             body = body)
        #logging.info('flushing to %s/stats, %s' % (self._host, body))
        httpclient.fetch(req, self.flushed)

    def process(self, key, data, opts = None):
        # called by the handler that receives the data
        meta, values = data
        if options.verbose > 0:
            opts_str = ''
            if opts and opts['Source']:
                opts_str = 'from %s' % opts['Source']
            if options.verbose > 2:
                self.log('processing data %s for %s, %s, %s' % (opts_str, key, meta, values))

        # Aggregate across sources!
        if key not in self._stats:
            meta = data[0]
            cls = globals()[meta[0]]
            if meta[0] == 'Gauge':
                opts['poll_interval'] = meta[1]
            instance = cls(key, **opts)
            logging.info('created %s with %s, %s' % (cls, key, opts))
            instance.merge_data(data, opts)
            self._stats[key] = instance
        else:
            instance = self._stats[key]
            #logging.info('merge data %s into %s' % (data, instance))
            instance.merge_data(data, opts)

    def push_new_data(self, key):
        #logging.info('push new data please for %s' % key)
        if key in self._listeners:
            closed_listeners = []
            for listener in self._listeners[key]:
                newvals = self._stats[key].push_new_values()
                if newvals:
                    #logging.info('got some newvals for %s, %s' % (key, newvals))
                    retval = listener.on_new_data(newvals)
                    if retval:
                        closed_listeners.append(listener)
            for todelete in closed_listeners:
                self._listeners[key].remove(closed_listeners)

    def flushed(self, response):
        if response.error:
            self.log('got flush response %s' % response)
        else:
            if options.verbose > 3:
                self.log('flushed data to %s with code %s' % (self._host, response.code))

    def register(self, stat):
        self._stats[stat.name] = stat

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
    def __init__(self, name, poll_interval=None, poll_fn=None, active=True, Source=None):

        self.frontend_push_deadline = None
        self.upstream_push_deadline = None

        if not active:
            return
        self._values = []
        self.active = active
        self._multivalues = {}
        self.name = name
        self.poll_interval = poll_interval
        self.poll_fn = poll_fn
        if self.poll_interval:
            if poll_fn:
                self.periodic = tornado.ioloop.PeriodicCallback( self.poll, self.poll_interval*1000 )
                self.periodic.start()
            else:
                # aggregating only!
                self.periodic = tornado.ioloop.PeriodicCallback( self.do_merge, self.poll_interval*1000 )
                self.periodic.start()
        manager.register(self)

    def meta(self):
        return ['Gauge',self.poll_interval]

    def poll(self):
        value = self.poll_fn()
        #logging.info('polling for stat %s, got value %s' % (self, value))
        self.add_value(value)

    def add_value(self, v, t=None):
        if t is None:t = time.time()
        self._values.append( (t,v) )
        manager.tick(t)

    def merge_data(self, data, opts):
        meta = data[0]
        poll_interval = meta[1]
        source = opts['Source']
        self.poll_interval = meta[1]
        if source not in self._multivalues:
            self._multivalues[source] = data[1]
        else:
            self._multivalues[source] += data[1]
        #logging.info('merged %s %s data %s' % (source, self.name, self._multivalues[source]))

    def do_merge(self):
        t = time.time()

        total = 0

        for source, arr_data in self._multivalues.iteritems():
            #logging.info('source %s has hist values len %s' % (source, len(arr_data)))
            found = False
            for i, datapoint in enumerate(arr_data):
                datatime = datapoint[0]
                reltime = (t - DEADLINE * self.poll_interval)
                diff = reltime - datatime
                #logging.info('%s: compare %s - %s = %s' % (i, reltime, datatime, diff))
                if diff > 0 and diff < self.poll_interval:
                    # use this datapoint! (and cut off any datapoints previous to this one)
                    found = True
                    break
            if found:
                total += datapoint[1]
                # arr_data = arr_data[i+1:] # does not mutate self._multivalues
                if i > 0:
                    logging.warn('cut off and lost some data! at index %s' % i)
                self._multivalues[source] = arr_data[i+1:]
        #logging.info('do merge adds value (%s,%s)' % (t, total))
        self.add_value(total, t)
        manager.push_new_data(self.name)

    def push_new_values(self):
        return self.consume( frontend_push = True )

    def ready_for_consume(self,t):
        if self._values:
            return True

    def consume(self, t=None, upstream_flush=None, frontend_push=None):
        if upstream_flush and 'service_identifier' not in options:
            #logging.info('raptor consume!')
            v = self._values
            self._values = []
            return v
        else:
            if t is None: t = time.time()
            if frontend_push:

                if self.frontend_push_deadline:
                    # consume only the values greater than the last push deadline
                    toreturn = filter(lambda x:x[0] > self.frontend_push_deadline, self._values)
                else:
                    # consume everything up to this point
                    toreturn = self._values

                self.frontend_push_deadline = t

            elif upstream_flush:

                if self.upstream_push_deadline:
                    # consume only the values greater than the last push deadline
                    toreturn = filter(lambda x:x[0] > self.upstream_push_deadline, self._values)
                else:
                    # consume everything up to this point
                    toreturn = self._values

                self.upstream_push_deadline = t

            #logging.info('consuming %s from values of len %s' % (len(toreturn), len(self._values)))

            self.delete_values_older_than( t - 2 * (DEADLINE + FLUSH_EVENTS) * self.poll_interval )

            return toreturn
            # do some garbage collection pls!!! !!! ! ! ! ! ! ! ! ! !!!!

    def delete_values_older_than(self, t):
        for i in range(len(self._values)):
            if self._values[i][0] < t:
                break

        self._values = self._values[i:]
                

    
class Count(object):
    def __init__(self, name, max_window = DEFAULT_COUNT_WINDOW, max_value=None, active=True):
        self.active = active
        self.max_window = max_window
        self.max_value = max_value
        self.name = name

        self.windows = []

        self.current_window_begin = time.time()
        self.counter = 0
        manager.register(self)

    def meta(self):
        return ['Count',self.max_window]

    def ready_for_consume(self, t=None):
        if t is None: t = time.time()
        return len(self.windows) > 0 or t - self.current_window_begin > self.max_window or (self.max_value and self.counter >= self.max_value)

    def merge_data(self, data, opts):
        pass

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
        if not self.active: return
        t = time.time()
        self.try_consume_window(val,t)

        #if self.ready_for_consume(t):
        #    manager.tick(t)
            
        

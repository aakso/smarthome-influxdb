#!/usr/bin/env python3
# vim: set encoding=utf-8 tabstop=4 softtabstop=4 shiftwidth=4 expandtab
#########################################################################
# Copyright 2016 Anton Aksola <aakso@iki.fi>                            /
#########################################################################
#  This file is part of SmartHome.py.    http://mknx.github.io/smarthome/
#
#  SmartHome.py is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  SmartHome.py is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with SmartHome.py. If not, see <http://www.gnu.org/licenses/>.
#########################################################################

import logging
import functools
import time
from datetime import timedelta as td
from datetime import datetime as dt
from collections import deque

# dependencies
import pyrfc3339
import influxdb
from influxdb import InfluxDBClient
import pytz

logger = logging.getLogger('')


class InfluxDB():
    MEASUREMENT_TPL = 'smarthome.{}'
    ITEM_CONF_KEY = 'influxdb'
    ITEM_CONF_KEY_NOSERIES = 'influxdb_no_series'
    TIME_SUFFIXES = {'m':'minutes', 'h':'hours', 'd':'days', 'y':'years'}

    MINUTE = 60
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR
    WEEK = 7 * DAY

    RESOLUTIONS = (
        (DAY, 5 * MINUTE),
        (WEEK, HOUR),
        (4 * WEEK, 12 * HOUR)
    )

    TIME_PRECISION = 'ms'

    EPOCH = dt.utcfromtimestamp(0)
    EPOCHTZ = dt.fromtimestamp(0, pytz.timezone('UTC'))

    def __init__(self, 
                 smarthome,
                 host=None,
                 database='smarthome',
                 user=None,
                 passwd=None,
                 port=8086,
                 cycle=300,
                 write_queue_max_size=1000,
                 flush_cycle=120):

        self._sh = smarthome
        self.clsname = self.__class__.__name__
        # DB write queue
        self._write_queue = deque([], int(write_queue_max_size))
        # Item registry
        self._items = set()

        c = InfluxDBClient(host, port, user, passwd, database)
        self._client = c
        self._db_admin = False
        
        # Try to connect and see if we have db admin rights or any rights
        try:
            c.get_list_users()
            self._db_admin = True
        except influxdb.client.InfluxDBClientError as e:
            if e.code == 403:
                self._db_admin = False
            else:
                logger.error("{}: Invalid username/password/database".format(self.clsname))
                self.alive = False
                return
        except Exception as e:
            logger.error("{}: Cannot connect to {}:{}, ({})" \
                         .format(self.clsname, host, port, e))
            self.alive = False
            return

        ## Init periodic tasks
        # Collect all item values to write queue
        smarthome.scheduler.add("{} value collect".format(self.clsname),
                               self._enqueue_all_items,
                               cycle=cycle,
                               offset=60,
                               prio=5)

        # Flush write queue to InfluxDB server periodically
        smarthome.scheduler.add("{} flush write queue".format(self.clsname),
                               self._flush_write_queue,
                               cycle=flush_cycle,
                               offset=70,
                               prio=5)

        #TODO(aakso): implement setup_continuous_queries

    def run(self):
        self.alive = True

    def stop(self):
        self.alive = False

    def _parse_timestr(self, timestr, reldt=None):
        timestr = str(timestr).strip().lower()
        if timestr == 'now': return dt.utcnow()
        # Timestr is unix ts
        if timestr.isdigit():
            return dt.utcfromtimestamp(int(timestr))

        # Handle relative input
        if not reldt: reldt = dt.utcnow()

        # Default operation is subtract
        op = reldt.__add__ if timestr.startswith('+') else reldt.__sub__

        suffix = timestr[-1]
        unit = self.TIME_SUFFIXES.get(suffix, None)

        if not unit:
            unit = 'hours'
            logger.warning('{}: Unrecognized time unit'.format(self.clsname))

        remchars = '+-'
        remchars += ''.join(self.TIME_SUFFIXES.keys()) 
        digits = timestr.strip(remchars)

        if not digits.isdigit():
            logger.error('{}: Cannot parse relative time: {}'\
                         .format(self.clsname, timestr))
            return None
        
        # Make timedelta object of the parsed timestring
        time = td(**{unit:int(digits)})

        # Do the calculation
        return op(time)

    def _get_resolution(self, secs):
        ret = None
        for th,res in sorted(self.RESOLUTIONS, reverse=True):
            if not ret or secs <= th: ret = res
        return ret
            
    def _dt_to_ts(self, t):
        return (t - self.EPOCH).total_seconds()

    def _dt_to_ts_tz(self, t):
        return (t - self.EPOCHTZ).total_seconds()

    def get_ts(self, precision=None):
        if not precision: precision = self.TIME_PRECISION
        ts = time.time()
        if precision == 's':
            return int(ts)
        elif precision == 'ms':
            return int(ts * 1000)
        elif precision == 'u':
            return int(ts * 1000 * 1000)
        else:
            return ts

    # Smarthome series func signature
    def _get_sh_series(self, func, start='-1h', end='now', count=100, ratio=1, update=False, step=None, sid=None, item=None):
        dt_start = self._parse_timestr(start)
        dt_end = self._parse_timestr(end)
        dt_diff = dt_end - dt_start
        ts_start = int(self._dt_to_ts(dt_start))
        ts_end = int(self._dt_to_ts(dt_end))

        # Initialize sh ws reply and its components
        if not sid:
            sid = '|'.join(str(a) for a in [item, func, start, end])
        reply = {
            'cmd': 'series',
            'series': [],
            'sid': sid,
        }

        if not step:
            step = self._get_resolution(ts_end - ts_start)
        else:
            step = int(step)

        data = self._get_item_series(item_name=item,
                                     start="{}s".format(ts_start),
                                     end="{}s".format(ts_end),
                                     time_window="{}s".format(step))
        measurement = self.MEASUREMENT_TPL.format(item)
        
        if func in ('avg', 'min', 'max'):
            select_field = "value_{}".format(func)
        elif func == 'on':
            select_field = "value_max"
        else:
            raise NotImplementedError

        # Construct update directive
        reply['params'] = {
            'update': True,
            'item': item,
            'func': func,
            # Start is the end of this query
            'start': ts_end,
            'end': ts_end + step,
            'step': step,
            'sid': sid
        }
        # We need to use smarthome's now() for this (tz aware)
        reply['update'] = self._sh.now() + td(seconds=step)


        if len(data) > 0:
            tuples = []
            for d in data.get_points(measurement):
                time = int(self._dt_to_ts_tz(pyrfc3339.parse(d['time'])))
                # Smartvisu expects time to be in microseconds
                time = time * 1000
                tuples.append([time, d[select_field]])

            tuples = sorted(tuples, key=lambda x: x[0])
            reply['series'] = tuples

        return reply
        
    def _flush_write_queue(self):
        qlen = len(self._write_queue)
        if qlen == 0: return

        logger.debug("{}: Flushing write queue (length: {})"\
                     .format(self.clsname, qlen))

        requeue = []
        while self._write_queue:
            # No locking should be required
            try:
                params = self._write_queue.popleft()
            except IndexError:
                break
            try:
                self._write_item(**params)
            except Exception as e:
                logger.error("{}: Exception {} while writing an item {}, requeueing"\
                             .format(self.clsname, e, params.get("name", None)))
                requeue.append(params)

        self._write_queue.extend(requeue)

    def _write_item(self, **params):
        clsname = self.__class__.__name__
        c = self._client
        name = params.get('name', None)
        val = params.get('value', None)
        ts = params.get('ts', self.get_ts())

        if not any(a for a in [int, float, bool] if type(val) == a):
            logger.error("{}: Invalid type given to _write_item"\
                         .format(clsname))
            return

        # Cast ints to floats for now
        if type(val) == int: val = float(val)

        # Construct write body
        point = {
        "fields": {
            "value": val
        },
        "measurement": self.MEASUREMENT_TPL.format(name),
        "time": ts
        }

        logger.debug(point)
        c.write_points([point], time_precision=self.TIME_PRECISION)


    def _get_item_series(self, **params):
        c = self._client
        item_name = params.get('item_name', None)
        q_args = {}
        q_args['start'] = params.get('start', 'now() - 1h')
        q_args['end'] = params.get('end', 'now()')
        q_args['time_window'] = params.get('time_window', '5m')
        q_args['series'] = self.MEASUREMENT_TPL.format(item_name)
        q = "select min(value) as value_min, "+\
            "mean(value) as value_avg, "+\
            "max(value) as value_max "+\
            'from "{series}" '+\
            "where time > {start} and time < {end} "+\
            "group by time({time_window}) fill(null)"

        try:
            r = c.query(q.format(**q_args))
        except Exception as e:
            logger.error("{}: Error while fetching: {}".format(self.clsname, e))
            return None
        if len(r) > 0:
            return r
        else:
            return None

    def _get_item_last_value(self, item_name):
        c = self._client
        measurement = self.MEASUREMENT_TPL.format(item_name)
        try:
            r = c.query('select * from "{}" limit 1'.format(measurement))
        except Exception as e:
            logger.error("{}: Error while fetching: {}".format(self.clsname, e))
            return None
        if len(r) > 0:
            return list(r.get_points(measurement))[0]
        else:
            return None

    def _inject_last_value(self, item):
        last = self._get_item_last_value(item.id())
        if last is None: return

        val = last['value']

        logger.debug("{}: {} <- value from DB ({})"\
                     .format(self.clsname, item.id(), val))
        item.set(val, self.clsname)

    def _enqueue_all_items(self):
        for item in self._items: self._enqueue_item(item)

    def _enqueue_item(self, item):
        self._write_queue.append(
            {'name': item.id(), 'value': item(), 'ts': self.get_ts()})

    def parse_item(self, item):
        if self.ITEM_CONF_KEY in item.conf:
            # Initial value injection
            if item.conf[self.ITEM_CONF_KEY] == 'init':
                self._inject_last_value(item)

            # Series method
            if not self.ITEM_CONF_KEY_NOSERIES in item.conf:
                # Attach series method
                item.series = functools.partial(self._get_sh_series, item=item.id())

            # Add item to registry for continuous polling
            self._items.add(item)

            logger.info("add item to queue: {0}".format(item.id()))

            # Return update callback to smarthome
            return self.update_item
        else:
            return None

    def update_item(self, item, caller=None, source=None, dest=None):
        if caller != "plugin":
            logger.info("update item: {0}".format(item.id()))
            self._enqueue_item(item)


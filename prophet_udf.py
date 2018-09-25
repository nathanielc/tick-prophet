#!/usr/bin/python

import os
import sys
import pandas as pd

from datetime import datetime
from kapacitor.udf.agent import Agent, Handler, Server
from kapacitor.udf import udf_pb2
from fbprophet import Prophet


import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()

version = '0.1'

# Process batches of data through the prophet forecasting model
class ProphetHandler(Handler):
    def __init__(self, agent):
        self._agent = agent
        self._field = 'value'
        self._capacity_field = ''
        self._periods = 0
        self._as = self._field
        self._as_lower = self._as + '_lower'
        self._as_upper = self._as + '_upper'
        self._version_tag = ''

        self._interval_width = 0.80
        self._changepoint_prior_scale = 0.05


        self._cap_prev = 0
        self._cs = []
        self._ys = []
        self._ts = []

        self._begin_response = None

    def info(self):
        response = udf_pb2.Response()
        response.info.wants = udf_pb2.BATCH
        response.info.provides = udf_pb2.BATCH
        response.info.options['field'].valueTypes.append(udf_pb2.STRING)
        response.info.options['capacity'].valueTypes.append(udf_pb2.STRING)
        response.info.options['periods'].valueTypes.append(udf_pb2.INT)
        response.info.options['as'].valueTypes.append(udf_pb2.STRING)
        response.info.options['asLower'].valueTypes.append(udf_pb2.STRING)
        response.info.options['asUpper'].valueTypes.append(udf_pb2.STRING)
        response.info.options['versionTag'].valueTypes.append(udf_pb2.STRING)
        response.info.options['intervalWidth'].valueTypes.append(udf_pb2.DOUBLE)
        response.info.options['changepointPriorScale'].valueTypes.append(udf_pb2.DOUBLE)
        return response

    def init(self, init_req):
        success = True
        msg = []
        for opt in init_req.options:
            if opt.name == 'field':
                self._field = opt.values[0].stringValue
            if opt.name == 'capacity':
                self._capacity_field = opt.values[0].stringValue
            elif opt.name == 'periods':
                self._periods = opt.values[0].intValue
            elif opt.name == 'as':
                self._as = opt.values[0].stringValue
            elif opt.name == 'asLower':
                self._as_lower = opt.values[0].stringValue
            elif opt.name == 'asUpper':
                self._as_upper = opt.values[0].stringValue
            elif opt.name == 'versionTag':
                self._version_tag = opt.values[0].stringValue
            elif opt.name == 'intervalWidth':
                self._interval_width = opt.values[0].doubleValue
            elif opt.name == 'changepointPriorScale':
                self._changepoint_prior_scale = opt.values[0].doubleValue

        if self._field == '':
            success = False
            msg.append('field name cannot be empty')
        if self._periods <= 0:
            success = False
            msg.append('periods must be positive')
        if self._as == '':
            success = False
            msg.append('as name cannot be empty')
        if self._as_lower == '':
            success = False
            msg.append('asLower name cannot be empty')
        if self._as_upper == '':
            success = False
            msg.append('asUpper name cannot be empty')
        if self._interval_width <= 0 :
            success = False
            msg.append('intervalWwidth must be positive')
        if self._changepoint_prior_scale <= 0 :
            success = False
            msg.append('changepointPriorScale must be positive')



        response = udf_pb2.Response()
        response.init.success = success
        response.init.error = '\n'.join(msg)
        return response

    def snapshot(self):
        response = udf_pb2.Response()
        response.snapshot.snapshot = ''
        return response

    def restore(self, restore_req):
        response = udf_pb2.Response()
        response.restore.success = False
        response.restore.error = 'not implemented'
        return response

    def begin_batch(self, begin_req):
        self._cs = []
        self._ys = []
        self._ts = []

        # Keep copy of begin_batch
        response = udf_pb2.Response()
        response.begin.CopyFrom(begin_req)
        self._begin_response = response
        # Add version tag to data if requested
        if self._version_tag != '':
            self._begin_response.point.tags[self._version_tag] = version

    def point(self, point):
        self._ys.append(point.fieldsDouble[self._field])
        self._ts.append(pd.to_datetime(point.time))
        if self._capacity_field != '':
            cap = point.fieldsDouble[self._capacity_field]
            if  cap == 0:
                cap = self._cap_prev
            else:
                self._cap_prev = cap
            self._cs.append(cap)

    def end_batch(self, end_req):
        if len(self._ys) < self._periods*2:
            return
        # Use prophet to forecast time series
        df = None
        if len(self._cs) > 0:
            df = pd.DataFrame({'y':self._ys, 'ds':self._ts, 'cap': self._cs})
        else:
            df = pd.DataFrame({'y':self._ys, 'ds':self._ts})
        print(df.head())
        m = Prophet(
            interval_width=self._interval_width,
            changepoint_prior_scale=self._changepoint_prior_scale,
            #growth='linear',
        )
        m.fit(df)
        future = m.make_future_dataframe(periods=self._periods)

        forecast = m.predict(future)

        # Send forecasted data back to Kapacitor

        # Send begin batch with count of outliers
        self._begin_response.begin.size = self._periods
        self._agent.write_response(self._begin_response)

        # Send each point
        tmax = 0
        response = udf_pb2.Response()
        response.point.name = self._begin_response.begin.name
        for k in self._begin_response.begin.tags:
            response.point.tags[k] = self._begin_response.begin.tags[k]

        for index, row in forecast.iloc[len(self._ys):].iterrows():
            t = row['ds'].value
            response.point.time = t
            response.point.fieldsDouble[self._as] = float(row['yhat'])
            response.point.fieldsDouble[self._as_lower] = float(row['yhat_lower'])
            response.point.fieldsDouble[self._as_upper] = float(row['yhat_upper'])
            self._agent.write_response(response)

            tmax = t

        # End the batch
        response = udf_pb2.Response()
        response.end.CopyFrom(end_req)
        response.end.tmax = tmax
        self._agent.write_response(response)


class accepter(object):
    _count = 0
    def accept(self, conn, addr):
        self._count += 1
        a = Agent(conn, conn)
        h = ProphetHandler(a)
        a.handler = h

        logger.info("Starting Agent for connection %d", self._count)
        a.start()
        a.wait()
        logger.info("Agent finished connection %d",self._count)

#if __name__ == '__main__':
#    a = Agent()
#    h = ProphetHandler(a)
#    a.handler = h
#
#    logger.info("Starting Agent")
#    a.start()
#    a.wait()
#    logger.info("Agent finished")

if __name__ == '__main__':
    path = "/tmp/prophet.sock"
    if len(sys.argv) == 2:
        path = sys.argv[1]
    server = Server(path, accepter())
    logger.info("Started server")
    server.serve()

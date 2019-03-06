
import streamsx.hbase as hbase

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.toolkit as tk
import streamsx.rest as sr

import unittest
import datetime
import os
import json

##
## Test assumptions
##
## Streaming analytics service or Streams instance running
## IBM cloud Analytics Engine service credentials are located in a file referenced by environment variable ANALYTICS_ENGINE.
## The core-site.xml is referenced by HBASE_SITE_XML environment variable.
## HBASE toolkit location is given by STREAMS_HBASE_TOOLKIT environment variable.
##
def toolkit_env_var():
    result = True
    try:
        os.environ['STREAMS_HBASE_TOOLKIT']
    except KeyError: 
        result = False
    return result

def streams_install_env_var():
    result = True
    try:
        os.environ['STREAMS_INSTALL']
    except KeyError: 
        result = False
    return result

def site_xml_env_var():
    result = True
    try:
        os.environ['HBASE_SITE_XML']
    except KeyError: 
        result = False
    return result

def hadoop_host_port_env_var():
    result = True
    try:
        os.environ['HADOOP_HOST_PORT']
    except KeyError: 
        result = False
    return result

def cloud_creds_env_var():
    result = True
    try:
        os.environ['ANALYTICS_ENGINE']
    except KeyError: 
        result = False
    return result

def _create_stream_for_get(topo):
    s = topo.source([1,2,3,4,5,6])
    schema=StreamSchema('tuple<int32 id, rstring who, rstring infoType, rstring requestedDetail>').as_tuple()
    return s.map(lambda x : (x,'Gandalf', 'location','beginTwoTowers'), schema=schema)


class StringData(object):
    def __init__(self, who, count, delay=True):
        self.who = who
        self.count = count
        self.delay = delay
    def __call__(self):
        if self.delay:
            time.sleep(10)
        for i in range(self.count):
            yield self.who



class TestParams(unittest.TestCase):

    @unittest.skipIf(hadoop_host_port_env_var() == False, "Missing HADOOP_HOST_PORT environment variable.")
    def test_xml_creds(self):
        topo = Topology()
        hbase.scan(topo, table_name='streamsSample_lotr', max_versions=3)



class TestDistributed(unittest.TestCase):
    """ Test in local Streams instance with local toolkit from STREAMS_HBASE_TOOLKIT environment variable """

    @classmethod
    def setUpClass(self):
        print (str(self))

    def setUp(self):
        Tester.setup_distributed(self)
        self.hbase_toolkit_location = os.environ['STREAMS_HBASE_TOOLKIT']
 
     # ------------------------------------
    @unittest.skipIf(hadoop_host_port_env_var() == False, "HADOOP_HOST_PORT environment variable.")
    def test_hbase_scan(self):
        topo = Topology('test_hbase_scan')

        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)
 
        topo = Topology()

        scanned_rows = hbase.scan(topo, table_name='streamsSample_lotr', max_versions=1 , init_delay=2)
        scanned_rows.print()


        tester = Tester(topo)
        tester.tuple_count(scanned_rows, 2, exact=False)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

    def test_hbase_get(self):
        topo = Topology('test_hbase_get')

        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)
        s = _create_stream_for_get(topo) 
        get_rows = hbase.get(s, table_name='streamsSample_lotr', row_attr_name='who')
        get_rows.print()


        tester = Tester(topo)
        tester.tuple_count(get_rows, 1, exact=False)
#        tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)


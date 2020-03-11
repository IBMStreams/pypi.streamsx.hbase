# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import streamsx.hbase as hbase

from streamsx.topology.topology import streamsx, Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import StreamSchema
import streamsx.spl.toolkit as tk
import streamsx.spl.op as op


import unittest
import os
import time

##
## Test assumptions
## Before you start with test make sure that:
##
## - IBM Streams instance running
## - HBASE server running 
## - The hostname and the port of HBASE server is referenced by HADOOP_HOST_PORT environment variable.
## - Or the "core-site.xml" is referenced by HBASE_SITE_XML environment variable.
## - HBASE toolkit location is given by STREAMS_HBASE_TOOLKIT environment variable.
## - A table 'streamsSample_lotr' is exsit on your HBASE database.
##    echo "create 'streamsSample_lotr','appearance','location'" | hbase shell
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
#        print ("Missing HADOOP_HOST_PORT environment variable.")
    return result

def cloud_creds_env_var():
    result = True
    try:
        os.environ['ANALYTICS_ENGINE']
    except KeyError: 
        result = False
    return result


def _create_stream_for_get(topo):
    # create 10 rows as query     
    createRows = op.Source(topo, "spl.utility::Beacon", 'tuple<int32 id, rstring who, rstring colF, rstring colQ>', params = {'period':0.01, 'iterations':10})
    createRows.id = createRows.output('(int32) IterationCount()')
    createRows.who = createRows.output('"Gandalf_" + (rstring) IterationCount()')
    createRows.colF = createRows.output('"location"')
    createRows.colQ = createRows.output('"beginTwoTowers"')
    return createRows.stream

def _get_timestamp():
    # get current time in mili seconds as Timestamp 
    timeStamp = int(time.time()*1000.0)
    return timeStamp


def _create_stream_for_put(topo):
    # create 10 rows        
    createRows = op.Source(topo, "spl.utility::Beacon", 'tuple<int32 id, rstring character, rstring colF, rstring colQ, rstring value, int64 Timestamp>', 
                           params = {'period':0.01, 'iterations':10}, 
                           name = 'createRows')
    createRows.id = createRows.output('(int32) IterationCount()')
    createRows.character = createRows.output('"Gandalf_" + (rstring) IterationCount()')
    createRows.colF = createRows.output('"location"')
    createRows.colQ = createRows.output('"beginTwoTowers"')
    createRows.value = createRows.output('"travelling_" + (rstring) IterationCount()')
    createRows.Timestamp = createRows.output('(int64)getTimestampInSecs() + (int64)IterationCount()')
    return createRows.stream


def _get_table_name():
    tableName = 'streamsSample_lotr'
    return tableName


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

    def test_hadoop_host_port(self):
        topo = Topology()
        hbase.scan(topo, table_name=_get_table_name(), max_versions=3)
        s = _create_stream_for_get(topo) 
        hbase.get(s, table_name=_get_table_name(), row_attr_name='who')



class TestDistributedPut(unittest.TestCase):
    """ Test in local Streams instance with local toolkit from STREAMS_HBASE_TOOLKIT environment variable """

    @classmethod
    def setUpClass(self):
        print (str(self))

    def setUp(self):
        Tester.setup_distributed(self)
        self.hbase_toolkit_location = os.environ['STREAMS_HBASE_TOOLKIT']

    # -----------------------------------------------------------
    def test_hbase_put(self):
        topo = Topology('test_hbase_put')

        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)
        print ('\n--------------------------- \nTest :   ' + topo.name + '\n--------------------------- ')

        s = _create_stream_for_put(topo) 
        s.print()
        tester = Tester(topo)
        # hbase.put creates site.xml file with the use of environment variable HADOOP_HOST_PORT / HBASE_SITE_XML
        put_rows = hbase.put(s, table_name=_get_table_name())
        put_rows.print(name='printPut')

        tester.tuple_count(put_rows, 2, exact=False)
        # tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)



class TestCompositeClass(unittest.TestCase):
    """ Test in local Streams instance with local toolkit from STREAMS_HBASE_TOOLKIT environment variable """

    @classmethod
    def setUpClass(self):
        print (str(self))

    def setUp(self):
        Tester.setup_distributed(self)
        self.hbase_toolkit_location = os.environ['STREAMS_HBASE_TOOLKIT']
    
    # -----------------------------------------------------------
    def test_hbase_put_composite(self):
        topo = Topology('hbase_put_composite')
        print ('\n--------------------------- \nTest :   ' + topo.name + '\n--------------------------- ')


        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)
        inputStream = _create_stream_for_put(topo) 
        tester = Tester(topo)

        hbase_put_output_schema = StreamSchema('tuple<boolean success>')

        # Additional optional parameters as variable keyword arguments for HBasePut
        options = {
          'columnFamilyAttrName' : 'colF',
          'columnQualifierAttrName' : "colQ",
          'TimestampAttrName' : 'Timestamp',
          'successAttr' : 'success'  
        }       


        # hbase.HBasePut creates site.xml file with the use of environment variable HADOOP_HOST_PORT / HBASE_SITE_XML and copy it in etc directory

        put_rows = inputStream.map(hbase.HBasePut(tableName='streamsSample_lotr', rowAttrName='character', valueAttrName='value', schema=hbase_put_output_schema, **options))
 
        put_rows.print(name='printPut')

        tester.tuple_count(put_rows, 2, exact=False)
        # tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)
    
       
    # -----------------------------------------------------------
    def test_hbase_get_composite(self):
        topo = Topology('test_hbase_get_composite')
        print ('\n--------------------------- \nTest :   ' + topo.name + '\n--------------------------- ')

        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)
        tester = Tester(topo)
  
        inputStream = _create_stream_for_get(topo) 
        # hbase.get creates site.xml file with the use of environment variable HADOOP_HOST_PORT / HBASE_SITE_XML

        output_schema = StreamSchema('tuple<rstring who, rstring colF, rstring colQ, rstring value, int32 numResults>')

        # Additional optional parameters as variable keyword arguments for HBaseGet
        options = {
          'columnFamilyAttrName' : 'colF',
          'columnQualifierAttrName' : "colQ",
          'outAttrName' : "value" ,
          'outputCountAttr' : 'numResults',
          'maxVersions' : 0 
        }       

        get_rows = inputStream.map(hbase.HBaseGet(tableName=_get_table_name(), rowAttrName='who', schema=output_schema, **options))
        get_rows.print(name='printGet')
 
        # tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)
        
    # -------------------------------------------------------------------------
    def test_hbase_scan_composite(self):
        topo = Topology('test_hbase_scan_composite')
        print ('\n--------------------------- \nTest :   ' + topo.name + '\n--------------------------- ')


        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)

        tester = Tester(topo)

        output_schema = StreamSchema('tuple<rstring row, int32 numResults, rstring columnFamily, rstring columnQualifier, rstring value>')

        # Additional optional parameters as variable keyword arguments for HBaseScan.
        options = {
            'initDelay': 2.0,
            'outAttrName' : 'value',
            'outputCountAttr' : 'numResults', 
            'maxVersions' : 0
         }       


        # hbase.HBaseScan creates site.xml file with the use of environment variable HADOOP_HOST_PORT / HBASE_SITE_XML

        scanned_rows = topo.source(hbase.HBaseScan(tableName=_get_table_name(), schema=output_schema,  **options))

        scanned_rows.print(name='printScan')

        tester.tuple_count(scanned_rows, 2, exact=False)
        #tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)



class TestDistributed(unittest.TestCase):
    """ Test in local Streams instance with local toolkit from STREAMS_HBASE_TOOLKIT environment variable """

    @classmethod
    def setUpClass(self):
        print (str(self))

    def setUp(self):
        Tester.setup_distributed(self)
        self.hbase_toolkit_location = os.environ['STREAMS_HBASE_TOOLKIT']

    # -----------------------------------------------------------
    def test_hbase_put(self):
        topo = Topology('test_hbase_put')
        print ('\n--------------------------- \nTest :   ' + topo.name + '\n--------------------------- ')


        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)
        s = _create_stream_for_put(topo) 
        tester = Tester(topo)
        # hbase.put creates site.xml file with the use of environment variable HADOOP_HOST_PORT / HBASE_SITE_XML
        put_rows = hbase.put(s, table_name=_get_table_name())
        put_rows.print(name='printPut')

        tester.tuple_count(put_rows, 2, exact=False)
        # tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

 
    # -----------------------------------------------------------
    def test_hbase_get(self):
        topo = Topology('test_hbase_get')
        print ('\n--------------------------- \nTest :   ' + topo.name + '\n--------------------------- ')

        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)

        s = _create_stream_for_get(topo) 
        tester = Tester(topo)
        # hbase.get creates site.xml file with the use of environment variable HADOOP_HOST_PORT / HBASE_SITE_XML
        get_rows = hbase.get(s, table_name=_get_table_name(), row_attr_name='who')
        get_rows.print(name='printGet')


        tester.tuple_count(get_rows, 2, exact=False)
        # tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)


    # --------------------------------------------------------
    def test_hbase_scan(self):
        topo = Topology('test_hbase_scan')
        print ('\n--------------------------- \nTest :   ' + topo.name + '\n--------------------------- ')

        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)
 
        tester = Tester(topo)
        # hbase.scan creates site.xml file with the use of environment variable HADOOP_HOST_PORT / HBASE_SITE_XML
        scanned_rows = hbase.scan(topo, table_name=_get_table_name(), max_versions=1 , init_delay=2)
        scanned_rows.print(name='printScan')

        tester.tuple_count(scanned_rows, 2, exact=False)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

    # --------------------------------------------------------
    @unittest.skipIf(((site_xml_env_var()))== False, "Missing HBASE_SITE_XML")
    def test_hbase_scan_connection_param_file(self):
        topo = Topology('test_hbase_scan_connection_param_file')
        print ('\n--------------------------- \nTest :   ' + topo.name + '\n--------------------------- ')

        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)
 
        tester = Tester(topo)
        # set HBASE_SITE_XML file as connection parameter
        scanned_rows = hbase.scan(topo, table_name=_get_table_name(), max_versions=1 , init_delay=2, connection=os.environ['HBASE_SITE_XML'])
        scanned_rows.print(name='printScan')

        tester.tuple_count(scanned_rows, 2, exact=False)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)


    # --------------------------------------------------------
    @unittest.skipIf(((hadoop_host_port_env_var()))== False, "Missing HADOOP_HOST_PORT")
    def test_hbase_scan_connection_param_string(self):
        topo = Topology('test_hbase_scan_connection_param_string')
        print ('\n--------------------------- \nTest :   ' + topo.name + '\n--------------------------- ')

        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)
 
        tester = Tester(topo)
        # set HADOOP_HOST_PORT string as connection parameter
        scanned_rows = hbase.scan(topo, table_name=_get_table_name(), max_versions=1 , init_delay=2, connection=os.environ['HADOOP_HOST_PORT'])
        scanned_rows.print(name='printScan')


        tester.tuple_count(scanned_rows, 2, exact=False)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

    # --------------------------------------------------------
    @unittest.skipIf(((hadoop_host_port_env_var()))== False, "Missing HADOOP_HOST_PORT")
    def test_hbase_scan_connection_param_dict(self):
        topo = Topology('test_hbase_scan_connection_param_dict')
        print ('\n--------------------------- \nTest :   ' + topo.name + '\n--------------------------- ')

        if self.hbase_toolkit_location is not None:
            tk.add_toolkit(topo, self.hbase_toolkit_location)
 
        tester = Tester(topo)
        # set dict as connection parameter with values from HADOOP_HOST_PORT
        hp = os.environ['HADOOP_HOST_PORT'].split(":", 1)
        ext_connection = {}
        ext_connection['host'] = hp[0]
        ext_connection['port'] = hp[1]
        scanned_rows = hbase.scan(topo, table_name=_get_table_name(), max_versions=1 , init_delay=2, connection=ext_connection)
        scanned_rows.print(name='printScan')

        tester.tuple_count(scanned_rows, 2, exact=False)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)


class TestICPRemote(TestDistributed):
    def setUp(self):
        Tester.setup_distributed(self)
        self.hbase_toolkit_location = None


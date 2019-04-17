# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

"""
Overview
++++++++

Provides functions to access files on HBASE.


HBASE configuration file
+++++++++++++++++++++++++



Package can be tested with TopologyTester using the IBM Streams.


The host name and the port of hadoop server has to be specified for testing with the environment variable **HADOOP_HOST_PORT**.

For example::

    
     export HADOOP_HOST_PORT=hdp264.fyre.ibm.com:8020


The package creates a HBase configuration file (hbase-site.xml) from a template.

And replaces the hadoop server name and the port with values from environment variable **HADOOP_HOST_PORT**.

Alternative is specify the location of HBase configuration file **hbase-site.xml** with the environment variable **HBASE_SITE_XML**.

For example::


    export HBASE_SITE_XML=/usr/hdp/current/hbase-client/conf/hbase-site.xml

 
The location of hbase toolkit has to be specified for testing with the environment variable **STREAMS_HBASE_TOOLKIT**.



For example::

    export STREAMS_HBASE_TOOLKIT=/opt/ibm/InfoSphere_Streams/4.3.0.0/toolkits/com.ibm.streamsx.hbase


                                   

    
Sample
++++++



A simple scan example of a Streams application scans the contains a hbase table and returns
the results in scanned_rows::

    from streamsx.topology.topology import *
    from streamsx.topology.schema import CommonSchema, StreamSchema
    from streamsx.topology.context import submit
    import streamsx.hbase as hbase

    topo = Topology('test_hbase_scan')
    if self.hbase_toolkit_location is not None:
        tk.add_toolkit(topo, self.hbase_toolkit_location)

    if (hbase.generate_hbase_site_xml(topo)):
        tester = Tester(topo)
        scanned_rows = hbase.scan(topo, table_name=_get_table_name(), max_versions=1 , init_delay=2)
        scanned_rows.print()
        tester.tuple_count(scanned_rows, 2, exact=False)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     

        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)
    else:
       print("hbase_site_xml file doesn't exist")
    

    
    
    
"""

__version__='1.0.0'

__all__ = ['generate_hbase_site_xml', 'scan', 'get', 'put', 'delete']
from streamsx.hbase._hbase import generate_hbase_site_xml, scan, get, put, delete

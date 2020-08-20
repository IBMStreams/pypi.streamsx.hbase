# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

"""
Overview
++++++++

Provides functions to access HBASE.


HBASE configuration file
+++++++++++++++++++++++++


The host name and the port of hadoop server has to be specified with the environment variable **HADOOP_HOST_PORT**.

For example::

    
     export HADOOP_HOST_PORT=hdp264.fyre.ibm.com:8020


The package creates a HBase configuration file (hbase-site.xml) from a template.

And replaces the hadoop server name and the port with values from environment variable **HADOOP_HOST_PORT**.

Alternative is specify the location of HBase configuration file **hbase-site.xml** with the environment variable **HBASE_SITE_XML**.

For example::

    export HBASE_SITE_XML=/usr/hdp/current/hbase-client/conf/hbase-site.xml
                                 

    
Sample
++++++



A simple Streams application that scans a HBASE table and prints
the scanned rows::

    from streamsx.topology.topology import *
    from streamsx.topology.schema import CommonSchema, StreamSchema
    from streamsx.topology.context import submit
    import streamsx.hbase as hbase

    topo = Topology('hbase_scan_sample')

    scanned_rows = hbase.scan(topo, table_name='sample', max_versions=1 , init_delay=2)
    scanned_rows.print()

    cfg = {}
    cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     
    submit ('DISTRIBUTED', topo, cfg) 
    
    
"""

__version__='1.5.1'

__all__ = ['HBaseGet', 'HBasePut', 'HBaseScan', 'download_toolkit', 'scan', 'get', 'put', 'delete']
from streamsx.hbase._hbase import download_toolkit, scan, get, put, delete, HBaseGet, HBasePut, HBaseScan


# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

"""
Overview
++++++++

Provides functions to access files on HBASE.

Use this package with the following services on IBM Cloud:

  * `Streaming Analytics <https://www.ibm.com/cloud/streaming-analytics>`_
  * `Analytics Engine <https://www.ibm.com/cloud/analytics-engine>`_


Credentials
+++++++++++

"Analytics Engine" credentials are defined using service credentials JSON.

The mandatory JSON elements are "user", "password" and "webhbase"::

    {
        "cluster": {
            "password": "<PASSWORD>",
            "service_endpoints": {
                "webhbase": "https://<HOST>:<PORT>/gateway/default/webhbase/v1/"
            },
            "user": "<USER>"
        },
    }

If you are using HBASE server(s) different to the "Analytics Engine" service, 
then you can provide the  *configuration file* (``hbase-site.xml`` or ``core-site.xml``) to configure the connection.

Sample
++++++

A simple hello world example of a Streams application writing string messages to
a file to HBASE. Scan for created file on HBASE and read the content::

    from streamsx.topology.topology import *
    from streamsx.topology.schema import CommonSchema, StreamSchema
    from streamsx.topology.context import submit
    import streamsx.hbase as hbase

    credentials = json.load(credentials_analytics_engine_service)

    topo = Topology('HBASEHelloWorld')

    to_hbase = topo.source(['Hello', 'World!'])
    to_hbase = to_hbase.as_string()
   
    # Write a stream to HBASE
    hbase.write(to_hbase, credentials=credentials, file='/sample/hw.txt')

    scanned = hbase.scan(topo, credentials=credentials, directory='/sample', init_delay=10)
    
    # read text file line by line
    r = hbase.read(scanned, credentials=credentials)
    
    # print each line (tuple)
    r.print()

    submit('STREAMING_ANALYTICS_SERVICE', topo)

"""

__version__='1.0.0'

__all__ = ['scan', 'read', 'write']
from streamsx.hbase._hbase import scan, read, write

# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import datetime
import os
from tempfile import gettempdir
import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import StreamSchema
from streamsx.toolkits import download_toolkit

_TOOLKIT_NAME = 'com.ibm.streamsx.hbase'


HBASEScanOutputSchema = StreamSchema('tuple<rstring row, int32 numResults, rstring columnFamily, rstring columnQualifier, rstring value>')
"""Structured output schema of the scan response tuple. This schema is the output schema of the scan method.

``'tuple<rstring row, int32 numResults, rstring columnFamily, rstring columnQualifier, rstring value>'``
"""

HBASEGetOutputSchema = StreamSchema('tuple<rstring row, int32 numResults, rstring value, rstring infoType, rstring requestedDetail>')
"""Structured output schema of the get response tuple. This schema is the output schema of the get method.

``'tuple<rstring row, rstring value, rstring infoType, rstring requestedDetail>'``
"""

HBASEPutOutputSchema = StreamSchema('tuple<boolean success>')
"""Structured output schema of the put response tuple. This schema is the output schema of the put method.

``'tuple<boolean  success>'``
"""

def _add_toolkit_dependency(topo):
    # IMPORTANT: Dependency of this python wrapper to a specific toolkit version
    # This is important when toolkit is not set with streamsx.spl.toolkit.add_toolkit (selecting toolkit from remote build service)
    streamsx.spl.toolkit.add_toolkit_dependency(topo, 'com.ibm.streamsx.hbase', '[3.4.0,4.0.0)')




def _generate_hbase_site_xml(topo, connection=None):
    # The environment variable HADOOP_HOST_PORT has to be set.
    host_port = ""
    hbaseSiteXmlFile = ""
    if connection is None:
        # expect one of the environment variables HADOOP_HOST_PORT or HBASE_SITE_XML
        try:  
            host_port=os.environ['HADOOP_HOST_PORT']
        except KeyError: 
            host_port = ""

        try:  
            hbaseSiteXmlFile=os.environ['HBASE_SITE_XML']
        except KeyError: 
            hbaseSiteXmlFile = ""
    else:
        if isinstance(connection, dict): # check if dict is set
            host_port = connection.get('host') + ':' + connection.get('port')
        else:
            if os.path.exists(connection): # check if filename is given
                hbaseSiteXmlFile = connection
            else:
                if ':' in connection: # assume we have HOST:PORT as string
                    host_port = connection

    if (len(host_port) > 1):
        HostPort = host_port.split(":", 1)
        host = HostPort[0]
        port = HostPort[1]
        script_dir = os.path.dirname(os.path.realpath(__file__))
        hbaseSiteTemplate=script_dir + '/hbase-site.xml.temp'
        hbaseSiteXmlFile=gettempdir()+'/hbase-site.xml'

        # reads the hbase-site.xml.temp and replase the host and port
        with open(hbaseSiteTemplate) as f:
            newText=f.read().replace('HOST_NAME', host)
            newText=newText.replace('PORT', port)
    
        
        # creates a new file hbase-site.xml file with new host and port values
        with open(hbaseSiteXmlFile, "w") as f:
            f.write(newText)
        print ("HBase configuration xml file: " + hbaseSiteXmlFile + "   host: " + host + "   port: " + port)


    if (len(hbaseSiteXmlFile) > 2):
        if os.path.exists(hbaseSiteXmlFile):
            # add the HBase configuration file (hbase-site.xml) to the 'etc' directory in bundle
            topo.add_file_dependency(hbaseSiteXmlFile, 'etc')
            print ("HBase configuration xml file " + hbaseSiteXmlFile + ' added to the application directory.')
            return True
        else:
            raise AssertionError("The configuration file " + hbaseSiteXmlFile + " doesn't exists'")

    print ("Please set one of the environment variables HADOOP_HOST_PORT or HBASE_SITE_XML or apply the connection parameter")
    raise AssertionError("Missing HADOOP_HOST_PORT or HBASE_SITE_XML or connection parameter.")


def _check_time_param(time_value, parameter_name):
    if isinstance(time_value, datetime.timedelta):
        result = time_value.total_seconds()
    elif isinstance(time_value, int) or isinstance(time_value, float):
        result = time_value
    else:
        raise TypeError(time_value)
    if result <= 1:
        raise ValueError("Invalid "+parameter_name+" value. Value must be at least one second.")
    return result


def download_toolkit(url=None, target_dir=None):
    r"""Downloads the latest Hbase toolkit from GitHub.

    Example for updating the Hbase toolkit for your topology with the latest toolkit from GitHub::

        import streamsx.hbase as hbase
        # download Hbase toolkit from GitHub
        hbase_toolkit_location = hbase.download_toolkit()
        # add the toolkit to topology
        streamsx.spl.toolkit.add_toolkit(topology, hbase_toolkit_location)

    Example for updating the topology with a specific version of the Hbase toolkit using a URL::

        import streamsx.hbase as hbase
        url380 = 'https://github.com/IBMStreams/streamsx.hbase/releases/download/v3.8.0/streamsx.hbase.toolkits-3.8.0-20190829-1529.tgz'
        hbase_toolkit_location = hbase.download_toolkit(url=url380)
        streamsx.spl.toolkit.add_toolkit(topology, hbase_toolkit_location)

    Args:
        url(str): Link to toolkit archive (\*.tgz) to be downloaded. Use this parameter to 
            download a specific version of the toolkit.
        target_dir(str): the directory where the toolkit is unpacked to. If a relative path is given,
            the path is appended to the system temporary directory, for example to /tmp on Unix/Linux systems.
            If target_dir is ``None`` a location relative to the system temporary directory is chosen.

    Returns:
        str: the location of the downloaded Hbase toolkit

    .. note:: This function requires an outgoing Internet connection
    .. versionadded:: 1.3
    """
    _toolkit_location = streamsx.toolkits.download_toolkit (toolkit_name=_TOOLKIT_NAME, url=url, target_dir=target_dir)
    return _toolkit_location


def scan(topology, table_name, max_versions=None, init_delay=None, connection=None, name=None):
    """Scans a HBASE table and delivers the number of results, rows and values in output stream.
    
    The output streams has to be defined as StreamSchema.

    Args:
        topology(Topology): Topology to contain the returned stream.
        max_versions(int32): specifies the maximum number of versions that the operator returns. It defaults to a value of one. A value of 0 indicates that the operator gets all versions. 
        init_delay(int|float|datetime.timedelta): The time to wait in seconds before the operator scans the directory for the first time. If not set, then the default value is 0.
        connection(dict|filename|string): Specify the connection to HBASE either with a filename of a HBase configuration file or as string in format "HOST:PORT" or as dict containing the properties 'host' and 'port'. If not specified the environment variables ``HADOOP_HOST_PORT`` or ``HBASE_SITE_XML`` are used.
        name(str): Source name in the Streams context, defaults to a generated name.

    Returns:
        StreamSchema: Output Stream containing the row numResults and values. It is a structured streams schema.
        
        HBASEScanOutputSchema = StreamSchema('tuple<rstring row, int32 numResults, rstring columnFamily, rstring columnQualifier, rstring value>')
    """
    # check streamsx.hbase version
    _add_toolkit_dependency(topology)

    if (_generate_hbase_site_xml(topology, connection)):
        _op = _HBASEScan(topology, tableName=table_name, schema=HBASEScanOutputSchema, name=name)
    # configuration file is specified in hbase-site.xml. This file will be copied to the 'etc' directory of the application bundle.     
    #    topology.add_file_dependency(hbaseSite, 'etc')
        _op.params['hbaseSite'] = "etc/hbase-site.xml"
    
        if init_delay is not None:
            _op.params['initDelay'] = streamsx.spl.types.float64(_check_time_param(init_delay, 'init_delay'))

        _op.params['maxVersions'] = 0

        if max_versions is not None:
            _op.params['maxVersions'] = max_versions

        _op.params['outAttrName'] = "value" 
        _op.params['outputCountAttr'] = "numResults"

        return _op.outputs[0]


def get(stream, table_name, row_attr_name, connection=None, name=None):
    """get tuples from a HBASE table and delivers the number of results, rows and values in output stream.
    
    Args:
        stream: contain the input stream.
        table_name: The name of hbase table.
        row_attr_name(rstring): This parameter specifies the name of the attribute of the output port in which the operator puts the retrieval results. The data type for the attribute depends on whether you specified a columnFamily or columnQualifier.     
        connection(dict|filename|string): Specify the connection to HBASE either with a filename of a HBase configuration file or as string in format "HOST:PORT" or as dict containing the properties 'host' and 'port'. If not specified the environment variables ``HADOOP_HOST_PORT`` or ``HBASE_SITE_XML`` are used.
        name(str): Operator name in the Streams context, defaults to a generated name.

    Returns:
        StreamSchema: Output Stream containing the row numResults and values. It is a structured streams schema.
        
        HBASEGetOutputSchema = StreamSchema('tuple<rstring row, int32 numResults, rstring value, rstring infoType, rstring requestedDetail>')
    """
    # check streamsx.hbase version
    _add_toolkit_dependency(stream.topology)

    if (_generate_hbase_site_xml(stream.topology, connection)):
        _op = _HBASEGet(stream, tableName=table_name, rowAttrName=row_attr_name, schema=HBASEGetOutputSchema, name=name)
        # configuration file is specified in hbase-site.xml. This file will be copied to the 'etc' directory of the application bundle.     
        # stream.topology.add_file_dependency(hbaseSite, 'etc')
        _op.params['hbaseSite'] = "etc/hbase-site.xml"
    
        _op.params['outAttrName'] = "value" 
        _op.params['columnFamilyAttrName'] = "infoType" 
        _op.params['columnQualifierAttrName'] = "requestedDetail" 
        _op.params['outputCountAttr'] = "numResults"
        return _op.outputs[0]


def put(stream, table_name, connection=None, name=None):
    """put a row which delivers in streams as tuple into a HBASE table.
    
    The output streams has to be defined as StreamSchema.

    Args:
        stream: contain the input stream.
        table_name: The name of hbase table,
        connection(dict|filename|string): Specify the connection to HBASE either with a filename of a HBase configuration file or as string in format "HOST:PORT" or as dict containing the properties 'host' and 'port'. If not specified the environment variables ``HADOOP_HOST_PORT`` or ``HBASE_SITE_XML`` are used.
        name(str): Operator name in the Streams context, defaults to a generated name.

    Returns:
        StreamSchema: Output Stream containing the result sucesss.
        
        HBASEScanOutputSchema = StreamSchema('tuple<boolen success>')
    """

    # check streamsx.hbase version
    _add_toolkit_dependency(stream.topology)

    if (_generate_hbase_site_xml(stream.topology, connection)):
        _op = _HBASEPut(stream, tableName=table_name, schema=HBASEPutOutputSchema, name=name)
        # configuration file is specified in hbase-site.xml. This file will be copied to the 'etc' directory of the application bundle.     
        _op.params['hbaseSite'] = "etc/hbase-site.xml"
        _op.params['rowAttrName'] = "character" ;
        _op.params['valueAttrName'] = "value" 
        _op.params['columnFamilyAttrName'] = "colF" 
        _op.params['columnQualifierAttrName'] = "colQ" 
        _op.params['successAttr'] = "success"
        
    return _op.outputs[0]

def delete(stream, table_name, connection=None, name=None):
    """delete a row which delivers in streams as tuple from a HBASE table.
    
    The output streams has to be defined as StreamSchema.

    Args:
        stream: contain the input stream.
        table_name: The name of hbase table,
        connection(dict|filename|string): Specify the connection to HBASE either with a filename of a HBase configuration file or as string in format "HOST:PORT" or as dict containing the properties 'host' and 'port'. If not specified the environment variables ``HADOOP_HOST_PORT`` or ``HBASE_SITE_XML`` are used.
        name(str): Operator name in the Streams context, defaults to a generated name.

    Returns:
        StreamSchema: Output Stream containing the result sucesss.
        
        HBASEScanOutputSchema = StreamSchema('tuple<boolen success>')
    """

    # check streamsx.hbase version
    _add_toolkit_dependency(stream.topology)

    if (_generate_hbase_site_xml(stream.topology, connection)):
        _op = _HBASEDelete(stream, tableName=table_name, schema=HBASEScanOutputSchema, name=name)
        _op.params['hbaseSite'] = "etc/hbase-site.xml"
        _op.params['rowAttrName'] = "character" ;
        _op.params['valueAttrName'] = "value" 
        _op.params['columnFamilyAttrName'] = "colF" 
        _op.params['columnQualifierAttrName'] = "colQ" 
        _op.params['successAttr'] = "success"
 
    return _op.outputs[0]


# HBASEGet
# Required parameter: rowAttrName
# Optional parameters: authKeytab, authPrincipal, columnFamilyAttrName, columnQualifierAttrName, hbaseSite, maxVersions, 
# minTimestamp, outAttrName, outputCountAttr, staticColumnFamily, staticColumnQualifier, tableName, tableNameAttribute
class _HBASEGet(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, rowAttrName=None, authKeytab=None, authPrincipal=None, columnFamilyAttrName=None, columnQualifierAttrName=None, hbaseSite=None, maxVersions=None, minTimestamp=None, outAttrName=None, outputCountAttr=None, staticColumnFamily=None, staticColumnQualifier=None, tableName=None, tableNameAttribute=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hbase::HBASEGet"
        inputs=stream
        params = dict()
        if rowAttrName is not None:
            params['rowAttrName'] = rowAttrName
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if columnFamilyAttrName is not None:
            params['columnFamilyAttrName'] = columnFamilyAttrName
        if columnQualifierAttrName is not None:
            params['columnQualifierAttrName'] = columnQualifierAttrName
        if hbaseSite is not None:
            params['hbaseSite'] = hbaseSite
        if maxVersions is not None:
            params['maxVersions'] = maxVersions
        if minTimestamp is not None:
            params['minTimestamp'] = minTimestamp
        if outAttrName is not None:
            params['outAttrName'] = outAttrName
        if outputCountAttr is not None:
            params['outputCountAttr'] = outputCountAttr
        if staticColumnFamily is not None:
            params['staticColumnFamily'] = staticColumnFamily
        if staticColumnQualifier is not None:
            params['staticColumnQualifier'] = staticColumnQualifier
        if tableName is not None:
            params['tableName'] = tableName
        if tableNameAttribute is not None:
            params['tableNameAttribute'] = tableNameAttribute

        super(_HBASEGet, self).__init__(topology,kind,inputs,schema,params,name)


# HBASEScan
# Optional parameter: authKeytab, authPrincipal, channel, endRow, hbaseSite, initDelay, maxChannels, maxThreads, maxVersions, minTimestamp, 
# outAttrName, outputCountAttr, rowPrefix, startRow, staticColumnFamily, staticColumnQualifier, tableName, tableNameAttribute, triggerCount
class _HBASEScan(streamsx.spl.op.Invoke):
    def __init__(self, topology, schema=None, authKeytab=None, authPrincipal=None, channel=None, endRow=None, hbaseSite=None, initDelay=None, maxChannels=None, maxThreads=None,  maxVersions=None, minTimestamp=None, outAttrName=None, outputCountAttr=None, rowPrefix=None, startRow=None, staticColumnFamily=None, staticColumnQualifier=None, tableName=None, tableNameAttribute=None, triggerCount=None, name=None):
#        topology = stream.topology
        kind="com.ibm.streamsx.hbase::HBASEScan"
        inputs=None
        params = dict()
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if channel is not None:
            params['channel'] = channel
        if endRow is not None:
            params['endRow'] = endRow
        if hbaseSite is not None:
            params['hbaseSite'] = hbaseSite
        if initDelay is not None:
            params['initDelay'] = initDelay
        if maxChannels is not None:
            params['maxChannels'] = maxChannels
        if maxThreads is not None:
            params['maxThreads'] = maxThreads
        if maxVersions is not None:
            params['maxVersions'] = maxVersions
        if minTimestamp is not None:
            params['minTimestamp'] = minTimestamp
        if outAttrName is not None:
            params['outAttrName'] = outAttrName
        if outputCountAttr is not None:
            params['outputCountAttr'] = outputCountAttr
        if rowPrefix is not None:
            params['rowPrefix'] = rowPrefix
        if startRow is not None:
            params['startRow'] = startRow
        if staticColumnFamily is not None:
            params['staticColumnFamily'] = staticColumnFamily
        if staticColumnQualifier is not None:
            params['staticColumnQualifier'] = staticColumnQualifier
        if tableName is not None:
            params['tableName'] = tableName
        if tableNameAttribute is not None:
            params['tableNameAttribute'] = tableNameAttribute

        super(_HBASEScan, self).__init__(topology,kind,inputs,schema,params,name)



# HBASEPut
# Required parameters: rowAttrName, valueAttrName
# Optional parameters: authKeytab, authPrincipal, batchSize, checkAttrName, columnFamilyAttrName, columnQualifierAttrName, 
# enableBuffer, hbaseSite, staticColumnFamily, staticColumnQualifier, successAttr, tableName, tableNameAttribute
class _HBASEPut(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, rowAttrName=None, valueAttrName=None, authKeytab=None, authPrincipal=None, batchSize=None, checkAttrName=None, columnFamilyAttrName=None, columnQualifierAttrName=None, enableBuffer=None, hbaseSite=None, staticColumnFamily=None, staticColumnQualifier=None, successAttr=None, tableName=None, tableNameAttribute=None, name=None):
        kind="com.ibm.streamsx.hbase::HBASEPut"
        inputs=stream
        topology = stream.topology
        params = dict()
        if rowAttrName is not None:
            params['rowAttrName'] = rowAttrName
        if valueAttrName is not None:
            params['rowAttrName'] = valueAttrName
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if batchSize is not None:
            params['batchSize'] = batchSize
        if checkAttrName is not None:
            params['checkAttrName'] = checkAttrName
        if columnFamilyAttrName is not None:
            params['columnFamilyAttrName'] = columnFamilyAttrName
        if columnQualifierAttrName is not None:
            params['columnQualifierAttrName'] = columnQualifierAttrName
        if enableBuffer is not None:
            params['enableBuffer'] = enableBuffer
        if hbaseSite is not None:
            params['hbaseSite'] = hbaseSite
        if staticColumnFamily is not None:
            params['staticColumnFamily'] = staticColumnFamily
        if staticColumnQualifier is not None:
            params['staticColumnQualifier'] = staticColumnQualifier
        if successAttr is not None:
            params['successAttr'] = successAttr
        if tableName is not None:
            params['tableName'] = tableName
        if tableNameAttribute is not None:
            params['tableNameAttribute'] = tableNameAttribute

        super(_HBASEPut, self).__init__(topology,kind,inputs,schema,params,name)


# HBASEDelete
# Required parameter: rowAttrName
# Optional parameters: authKeytab, authPrincipal, batchSize, checkAttrName, columnFamilyAttrName, columnQualifierAttrName, deleteAllVersions, 
# hbaseSite, staticColumnFamily, staticColumnQualifier, successAttr, tableName, tableNameAttribute
class _HBASEDelete(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, rowAttrName=None, authKeytab=None, authPrincipal=None, batchSize=None, checkAttrName=None, columnFamilyAttrName=None, columnQualifierAttrName=None, deleteAllVersions=None, hbaseSite=None, staticColumnFamily=None, staticColumnQualifier=None, successAttr=None, tableName=None, tableNameAttribute=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hbase::HBASEDelete"
        inputs=stream
        params = dict()
        if rowAttrName is not None:
            params['rowAttrName'] = rowAttrName
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if batchSize is not None:
            params['batchSize'] = batchSize
        if checkAttrName is not None:
            params['checkAttrName'] = checkAttrName
        if columnFamilyAttrName is not None:
            params['columnFamilyAttrName'] = columnFamilyAttrName
        if columnQualifierAttrName is not None:
            params['columnQualifierAttrName'] = columnQualifierAttrName
        if deleteAllVersions is not None:
            params['deleteAllVersions'] = deleteAllVersions
        if hbaseSite is not None:
            params['hbaseSite'] = hbaseSite
        if staticColumnFamily is not None:
            params['staticColumnFamily'] = staticColumnFamily
        if staticColumnQualifier is not None:
            params['staticColumnQualifier'] = staticColumnQualifier
        if successAttr is not None:
            params['successAttr'] = successAttr
        if tableName is not None:
            params['tableName'] = tableName
        if tableNameAttribute is not None:
            params['tableNameAttribute'] = tableNameAttribute

        super(_HBASEDelete, self).__init__(topology,kind,inputs,schema,params,name)


# HBASEIncrement
# Required parameter: rowAttrName
# Optional parameters: authKeytab, authPrincipal, columnFamilyAttrName, columnQualifierAttrName, hbaseSite, 
# increment, incrementAttrName, staticColumnFamily, staticColumnQualifier, tableName, tableNameAttribute
class _HBASEIncrement(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, rowAttrName=None, authKeytab=None, authPrincipal=None, columnFamilyAttrName=None, columnQualifierAttrName=None, deleteAllVersions=None, hbaseSite=None,  increment=None, incrementAttrName=None, staticColumnFamily=None, staticColumnQualifier=None, successAttr=None, tableName=None, tableNameAttribute=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hbase::HBASEIncrement"
        inputs=stream
        params = dict()
        if rowAttrName is not None:
            params['rowAttrName'] = rowAttrName
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if columnFamilyAttrName is not None:
            params['columnFamilyAttrName'] = columnFamilyAttrName
        if columnQualifierAttrName is not None:
            params['columnQualifierAttrName'] = columnQualifierAttrName
        if deleteAllVersions is not None:
            params['deleteAllVersions'] = deleteAllVersions
        if hbaseSite is not None:
            params['hbaseSite'] = hbaseSite
        if increment is not None:
            params['increment'] = increment
        if incrementAttrName is not None:
            params['incrementAttrName'] = incrementAttrName
        if staticColumnFamily is not None:
            params['staticColumnFamily'] = staticColumnFamily
        if staticColumnQualifier is not None:
            params['staticColumnQualifier'] = staticColumnQualifier
        if successAttr is not None:
            params['successAttr'] = successAttr
        if tableName is not None:
            params['tableName'] = tableName
        if tableNameAttribute is not None:
            params['tableNameAttribute'] = tableNameAttribute

        super(_HBASEIncrement, self).__init__(topology,kind,inputs,schema,params,name)





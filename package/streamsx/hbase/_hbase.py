# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import datetime
import os
from tempfile import gettempdir
import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.toolkits import download_toolkit
import streamsx.topology.composite

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
    streamsx.spl.toolkit.add_toolkit_dependency(topo, 'com.ibm.streamsx.hbase', '[3.8.0,4.0.0)')




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
            host_port = connection.get('host') + ':' + str(connection.get('port'))
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
        hbaseSiteTemplate=script_dir + '/hbase-site.xml.template'
        hbaseSiteXmlFile=gettempdir()+'/hbase-site.xml'

        # reads the hbase-site.xml.template and replase the host and port
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
        _op.params['TimestampAttrName'] = "Timestamp"
        
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
class _HBASEGet(streamsx.spl.op.Invoke):
    """
        _HBASEGet
        The The HBASEGet operator gets tuples from an HBase table.
        Required parameter: rowAttrName
        Optional parameters: authKeytab, authPrincipal, columnFamilyAttrName, columnQualifierAttrName, hbaseSite, maxVersions, 
        minTimestamp, outAttrName, outputCountAttr, staticColumnFamily, staticColumnQualifier, tableName, tableNameAttribute
     """
    def __init__(self, stream, schema=None, rowAttrName=None, authKeytab=None, authPrincipal=None, columnFamilyAttrName=None, 
                 columnQualifierAttrName=None, hbaseSite=None, maxVersions=None, minTimestamp=None, outAttrName=None, outputCountAttr=None, 
                 staticColumnFamily=None, staticColumnQualifier=None, tableName=None, tableNameAttribute=None, vmArg=None, name=None):
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
        if vmArg is not None:
            params['vmArg'] = vmArg


        super(_HBASEGet, self).__init__(topology,kind,inputs,schema,params,name)


# HBASEScan
class _HBASEScan(streamsx.spl.op.Invoke):
    """
        _HBASEScan
        The HBASEScan operator scans an HBase table. Like the FileSource operator, it has an optional input port. 
        If no input port is specifed, then the operator scans the table according to the parameters that you specify, and sends the final punctuation. 
        If you specify an input port, the operator does not start a scan until it receives a tuple. 
        After the operator receives a tuple, it scans according to that tuple and produces a punctuation. 
        parameters: authKeytab, authPrincipal, channel, endRow, hbaseSite, initDelay, maxChannels, maxThreads, maxVersions, minTimestamp, 
        outAttrName, outputCountAttr, rowPrefix, startRow, staticColumnFamily, staticColumnQualifier, tableName, tableNameAttribute, triggerCount
    """
    
    def __init__(self, topology, schema=None, authKeytab=None, authPrincipal=None, channel=None, endRow=None, hbaseSite=None, initDelay=None, 
                 maxChannels=None, maxThreads=None,  maxVersions=None, minTimestamp=None, outAttrName=None, outputCountAttr=None, rowPrefix=None, 
                 startRow=None, staticColumnFamily=None, staticColumnQualifier=None, tableName=None, tableNameAttribute=None, triggerCount=None, vmArg=None, name=None):
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
        if vmArg is not None:
            params['vmArg'] = vmArg

        super(_HBASEScan, self).__init__(topology,kind,inputs,schema,params,name)



# HBASEPut
class _HBASEPut(streamsx.spl.op.Invoke):
    """
        _HBASEPut
        The HBASEPut operator puts tuples into an Hbase table. 
        Required parameters: rowAttrName, valueAttrName
        Optional parameters: authKeytab, authPrincipal, batchSize, checkAttrName, columnFamilyAttrName, columnQualifierAttrName, 
        enableBuffer, hbaseSite, staticColumnFamily, staticColumnQualifier, successAttr, tableName, tableNameAttribute
    """
    def __init__(self, stream, schema=None, rowAttrName=None, valueAttrName=None, authKeytab=None, authPrincipal=None, batchSize=None, checkAttrName=None, 
                 columnFamilyAttrName=None, columnQualifierAttrName=None, enableBuffer=None, hbaseSite=None, staticColumnFamily=None, staticColumnQualifier=None, 
                 successAttr=None, tableName=None, tableNameAttribute=None, Timestamp=None, TimestampAttrName=None, vmArg=None, name=None):
        kind="com.ibm.streamsx.hbase::HBASEPut"
        inputs=stream
        topology = stream.topology
        params = dict()
        if rowAttrName is not None:
            params['rowAttrName'] = rowAttrName
        if valueAttrName is not None:
            params['valueAttrName'] = valueAttrName
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
        if Timestamp is not None:
            params['Timestamp'] = Timestamp
        if TimestampAttrName is not None:
            params['TimestampAttrName'] = TimestampAttrName
        if vmArg is not None:
            params['vmArg'] = vmArg

        super(_HBASEPut, self).__init__(topology,kind,inputs,schema,params,name)


# HBASEDelete
class _HBASEDelete(streamsx.spl.op.Invoke):
    """
        _HBASEDelete
        The HBASEDelete operator deletes an entry, an entire row, a columnFamily in a row, or a columnFamily, columnQualifier pair in a row from an HBase table.
         Required parameters: rowAttrName
        Optional parameters: authKeytab, authPrincipal, batchSize, checkAttrName, columnFamilyAttrName, columnQualifierAttrName, deleteAllVersions, 
        hbaseSite, staticColumnFamily, staticColumnQualifier, successAttr, tableName, tableNameAttribute
    """
    def __init__(self, stream, schema=None, rowAttrName=None, authKeytab=None, authPrincipal=None, batchSize=None, checkAttrName=None, columnFamilyAttrName=None, 
                 columnQualifierAttrName=None, deleteAllVersions=None, hbaseSite=None, staticColumnFamily=None, staticColumnQualifier=None, successAttr=None, 
                 tableName=None, tableNameAttribute=None, vmArg=None, name=None):
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
        if vmArg is not None:
            params['vmArg'] = vmArg

        super(_HBASEDelete, self).__init__(topology,kind,inputs,schema,params,name)


# HBASEIncrement
class _HBASEIncrement(streamsx.spl.op.Invoke):
    """
        _HBASEIncrement
        The HBASEIncrement operator increments the specified HBase entry. The operator uses the HTable.increment function. 
        You can specify the value to increment as an operator parameter or as an attribute in the input tuple. 
        Required parameters: rowAttrName
        Optional parameters: authKeytab, authPrincipal, columnFamilyAttrName, columnQualifierAttrName, hbaseSite, 
        increment, incrementAttrName, staticColumnFamily, staticColumnQualifier, tableName, tableNameAttribute
    """
    def __init__(self, stream, schema=None, rowAttrName=None, authKeytab=None, authPrincipal=None, columnFamilyAttrName=None, columnQualifierAttrName=None, 
                 deleteAllVersions=None, hbaseSite=None,  increment=None, incrementAttrName=None, staticColumnFamily=None, staticColumnQualifier=None, 
                 successAttr=None, tableName=None, tableNameAttribute=None, vmArg=None, name=None):
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
        if vmArg is not None:
            params['vmArg'] = vmArg

        super(_HBASEIncrement, self).__init__(topology,kind,inputs,schema,params,name)



class HBaseGet(streamsx.topology.composite.Map):
    """
    HBaseGet gets tuples from an HBase table. 

    Example, gets tuples from HBase table 'streamsSample_lotr'::

        inputStream = _create_stream_for_get(topo) 
        # crete a query stream

        output_schema = StreamSchema('tuple<rstring who, rstring colF, rstring colQ, rstring value, int32 numResults>')

        options = {
          'columnFamilyAttrName' : 'colF',
          'columnQualifierAttrName' : "colQ",
          'outAttrName' : "value" ,
          'outputCountAttr' : 'numResults',
          'maxVersions' : 0 
        }       

        get_rows = inputStream.map(hbase.HBaseGet(tableName=_get_table_name(), rowAttrName='who', schema=output_schema, **options))
 

    Attributes
    ----------
    hbaseSite : dict|str
        The hbaseSite specifies the path of hbase-site.xml file. 
    tableName : str
        The name of HBase table.
    schema : StreamSchema
        Output schema, defaults to CommonSchema.String
    options : kwargs
        The additional optional parameters as variable keyword arguments.
    """

 
    def __init__(self, tableName, rowAttrName, connection=None, schema=CommonSchema.String, **options):
        self.schema = schema
        self.connection = connection
        self.rowAttrName = rowAttrName
        self.valueAttrName = None
        self.authKeytab = None
        self.authPrincipal = None
        self.columnFamilyAttrName = None
        self.columnQualifierAttrName = None
        self.hbaseSite = None
        self.maxVersions = None        
        self.minTimestamp = None
        self.outAttrName = None        
        self.outputCountAttr = None
        self.staticColumnFamily = None
        self.staticColumnQualifier = None
        self.tableName = tableName
        self.tableNameAttribute = None
        self.vmArg = None
  

        if 'rowAttrName' in options:
            self.rowAttrName = options.get('rowAttrName')
        if 'valueAttrName' in options:
            self.valueAttrName = options.get('valueAttrName')
        if 'authKeytab' in options:
            self.authKeytab = options.get('authKeytab')
        if 'authPrincipal' in options:
            self.authPrincipal = options.get('authPrincipal')
        if 'columnFamilyAttrName' in options:
            self.columnFamilyAttrName = options.get('columnFamilyAttrName')
        if 'columnQualifierAttrName' in options:
            self.columnQualifierAttrName = options.get('columnQualifierAttrName')
        if 'hbaseSite' in options:
            self.hbaseSite = options.get('hbaseSite')
        if 'maxVersions' in options:
            self.maxVersions = options.get('maxVersions')
        if 'minTimestamp' in options:
            self.minTimestamp = options.get('minTimestamp')
        if 'outAttrName' in options:
            self.outAttrName = options.get('outAttrName')
        if 'outputCountAttr' in options:
            self.outputCountAttr = options.get('outputCountAttr')
        if 'staticColumnFamily' in options:
            self.staticColumnFamily = options.get('staticColumnFamily')
        if 'staticColumnQualifier' in options:
            self.staticColumnQualifier = options.get('staticColumnQualifier')
        if 'tableName' in options:
            self.tableName = options.get('tableName')
        if 'tableNameAttribute' in options:
            self.tableNameAttribute = options.get('tableNameAttribute')
        if 'vmArg' in options:
            self.vmArg = options.get('vmArg')
  


    @property
    def rowAttrName(self):
        """
            str: Name of the attribute on the input tuple containing the row. It is required. 
        """
        return self._rowAttrName

    @rowAttrName.setter
    def rowAttrName(self, value):
        self._rowAttrName = value

    @property
    def authKeytab(self):
        """
            str: The optional parameter authKeytab specifies the file that contains the encrypted keys for the user that is specified by the authPrincipal parameter. The operator uses this keytab file to authenticate the user. The keytab file is generated by the administrator. You must specify this parameter to use Kerberos authentication.
        """
        return self._authKeytab

    @authKeytab.setter
    def authKeytab(self, value):
        self._authKeytab = value

    @property
    def authPrincipal(self):
        """
            str: The optional parameter authPrincipal specifies the Kerberos principal that you use for authentication. This value is set to the principal that is created for the IBM Streams instance owner. You must specify this parameter if you want to use Kerberos authentication. 
        """
        return self._authPrincipal

    @authPrincipal.setter
    def authPrincipal(self, value):
        self._authPrincipal = value


    @property
    def columnFamilyAttrName(self):
        """
            str: Name of the attribute on the input tuple containing the columnFamily. Cannot be used with staticColumnFmily. 
        """
        return self._columnFamilyAttrName

    @columnFamilyAttrName.setter
    def columnFamilyAttrName(self, value):
        self._columnFamilyAttrName = value

    @property
    def columnQualifierAttrName(self):
        """
            str: Name of the attribute on the input tuple containing the columnQualifier. Cannot be used with staticColumnQualifier.
        """
        return self._columnQualifierAttrName

    @columnQualifierAttrName.setter
    def columnQualifierAttrName(self, value):
        self._columnQualifierAttrName = value

    @property
    def hbaseSite(self):
        """
            str: The hbaseSite parameter specifies the path of hbase-site.xml file. This is the recommended way to specify the HBASE configuration. 
        """
        return self._hbaseSite

    @hbaseSite.setter
    def hbaseSite(self, value):
        self._hbaseSite = value



    @property
    def maxVersions(self):
        """
            int: The optional parameter maxVersions specifies the maximum number of versions that the operator returns. It defaults to a value of one. A value of 0 indicates that the operator gets all versions. 
        """
        return self._maxVersions

    @maxVersions.setter
    def maxVersions(self, value):
        self._maxVersions = value
     

    @property
    def minTimestamp(self):
        """
            int: The optional parameter minTimestamp specifies the minimum timestamp that is used for queries. The operator does not return any entries with a timestamp older than this value. Unless you specify the maxVersions parameter, the opertor returns only one entry in this time range. 
        """
        return self._minTimestamp

    @minTimestamp.setter
    def minTimestamp(self, value):
        self._minTimestamp = value


    @property
    def outAttrName(self):
        """
            str: The optional parameter outAttrName specifies the name of the attribute of the output port in which the operator puts the retrieval results. The data type for the attribute depends on whether you specified a columnFamily or columnQualifier. 
        """
        return self._outAttrName

    @outAttrName.setter
    def outAttrName(self, value):
        self._outAttrName = value


    @property
    def outputCountAttr(self):
        """
            str: The optional parameter outputCountAttr specifies the name of attribute of the output port where the operator puts a count of the values it populated. 
        """
        return self._outputCountAttr

    @outputCountAttr.setter
    def outputCountAttr(self, value):
        self._outputCountAttr = value


    @property
    def staticColumnFamily(self):
        """
            str: If this parameter is specified, it will be used as the columnFamily for all operations. (Compare to columnFamilyAttrName.) For HBASEScan, it can have cardinality greater than one. 
        """
        return self._staticColumnFamily

    @staticColumnFamily.setter
    def staticColumnFamily(self, value):
        self._staticColumnFamily = value

    @property
    def staticColumnQualifier(self):
        """
            str: If this parameter is specified, it will be used as the columnQualifier for all tuples. HBASEScan allows it to be specified multiple times. 
        """
        return self._staticColumnQualifier
    
    @staticColumnQualifier.setter
    def staticColumnQualifier(self, value):
        self._staticColumnQualifier = value


    @property
    def tableName(self):
        """
            str: Name of the HBASE table. It is an optional parameter but one of these parameters must be set in opeartor: 'tableName' or 'tableNameAttribute'. Cannot be used with 'tableNameAttribute'. If the table does not exist, the operator will throw an exception. 
        """
        return self._tableName

    @tableName.setter
    def tableName(self, value):
        self._tableName = value

    @property
    def tableNameAttribute(self):
        """
            str: Name of the attribute on the input tuple containing the tableName. Use this parameter to pass the table name to the operator via input port. Cannot be used with parameter 'tableName'. This is suitable for tables with the same schema. 
        """
        return self._tableNameAttribute

    @tableNameAttribute.setter
    def tableNameAttribute(self, value):
        self._tableNameAttribute = value


    @property
    def vmArg(self):
        """
            str: The optional parameter vmArg parameter to specify additional JVM arguments that are required by the specific invocation of the operator. 
        """
        return self._vmArg

    @vmArg.setter
    def vmArg(self, value):
        self._vmArg = value

    def populate(self, topology, stream, schema, name, **options):
  
        if self.maxVersions is not None:
            self.maxVersions = streamsx.spl.types.int32(self.maxVersions)
        if self.minTimestamp is not None:
            self.minTimestamp = streamsx.spl.types.int64(self.minTimestamp)
             
        # check streamsx.hbase version
        _add_toolkit_dependency(topology)

        if (_generate_hbase_site_xml(stream.topology, self.connection)):
            self.hbaseSite = 'etc/hbase-site.xml'
            _op = _HBASEGet(stream=stream, \
                        schema=self.schema, \
                        rowAttrName=self.rowAttrName, \
                        authKeytab=self.authKeytab, \
                        authPrincipal=self.authPrincipal, \
                        columnFamilyAttrName=self.columnFamilyAttrName, \
                        columnQualifierAttrName=self.columnQualifierAttrName, \
                        hbaseSite=self.hbaseSite, \
                        maxVersions=self.maxVersions, \
                        minTimestamp=self.minTimestamp, \
                        outAttrName=self.outAttrName, \
                        outputCountAttr=self.outputCountAttr, \
                        staticColumnFamily=self.staticColumnFamily, \
                        staticColumnQualifier=self.staticColumnQualifier, \
                        tableName=self.tableName, \
                        tableNameAttribute=self.tableNameAttribute, \
                        vmArg=self.vmArg, \
                        name=name)

            return _op.outputs[0]
        else:
            return None

class HBasePut(streamsx.topology.composite.Map):
    """
    HBasePut puts the incoming tuples into an Hbase table. 


    Example, puts tuples into HBase table 'streamsSample_lotr'::

        import streamsx.hbase as hbase

        output_schema = StreamSchema('tuple<rstring who, rstring infoType, rstring requestedDetail, rstring value, int32 numResults>')

        HBasePutParameters = {
          'columnFamilyAttrName' : 'infoType',
          'columnQualifierAttrName' : "requestedDetail",
          'HBasePutParameters' : "value" ,
         }       

        put_rows = inputStream.map(hbase.HBasePut(tableName='streamsSample_lotr', rowAttrName='who', schema=output_schema, **HBasePutParameters))
 
        get_rows.print()
 
 

    Attributes
    ----------
    hbaseSite : dict|str
        The hbaseSite specifies the path of hbase-site.xml file. .
    schema : StreamSchema
        Output schema, defaults to CommonSchema.String
    options : kwargs
        The additional optional parameters as variable keyword arguments.
    """

 
 #   Optional: Timestamp, TimestampAttrName, authKeytab, authPrincipal, batchSize, checkAttrName, columnFamilyAttrName, columnQualifierAttrName, enableBuffer, hbaseSite, 
 #   staticColumnFamily, staticColumnQualifier, successAttr, tableName, tableNameAttribute
    
    def __init__(self, tableName, rowAttrName, valueAttrName, connection=None, schema=CommonSchema.String, **options):
        self.schema = schema
        self.connection = connection
        self.rowAttrName = rowAttrName
        self.valueAttrName = valueAttrName
        self.authKeytab = None
        self.authPrincipal = None
        self.batchSize = None
        self.checkAttrName = None
        self.columnFamilyAttrName = None
        self.columnQualifierAttrName = None
        self.enableBuffer = None
        self.hbaseSite = None
        self.staticColumnFamily = None
        self.staticColumnQualifier = None
        self.successAttr = None
        self.tableName = tableName
        self.tableNameAttribute = None
        self.Timestamp = None
        self.TimestampAttrName = None
        self.vmArg = None
  

        if 'rowAttrName' in options:
            self.rowAttrName = options.get('rowAttrName')
        if 'valueAttrName' in options:
            self.valueAttrName = options.get('valueAttrName')
        if 'authKeytab' in options:
            self.authKeytab = options.get('authKeytab')
        if 'authPrincipal' in options:
            self.authPrincipal = options.get('authPrincipal')
        if 'batchSize' in options:
            self.batchSize = options.get('batchSize')
        if 'checkAttrName' in options:
            self.checkAttrName = options.get('checkAttrName')
        if 'columnFamilyAttrName' in options:
            self.columnFamilyAttrName = options.get('columnFamilyAttrName')
        if 'columnQualifierAttrName' in options:
            self.columnQualifierAttrName = options.get('columnQualifierAttrName')
        if 'hbaseSite' in options:
            self.hbaseSite = options.get('hbaseSite')
        if 'staticColumnFamily' in options:
            self.staticColumnFamily = options.get('staticColumnFamily')
        if 'staticColumnFamily' in options:
            self.staticColumnFamily = options.get('staticColumnFamily')
        if 'successAttr' in options:
            self.successAttr = options.get('successAttr')
        if 'tableName' in options:
            self.tableName = options.get('tableName')
        if 'tableNameAttribute' in options:
            self.tableNameAttribute = options.get('tableNameAttribute')
        if 'Timestamp' in options:
            self.Timestamp = options.get('Timestamp')
        if 'TimestampAttrName' in options:
            self.TimestampAttrName = options.get('TimestampAttrName')
        if 'vmArg' in options:
            self.vmArg = options.get('vmArg')
  

    @property
    def rowAttrName(self):
        """
            str: Name of the attribute on the input tuple containing the row. It is required. 
        """
        return self._rowAttrName

    @rowAttrName.setter
    def rowAttrName(self, value):
        self._rowAttrName = value


    @property
    def valueAttrName(self):
        """
            str: This parameter specifies the name of the attribute that contains the value that is put into the table. It is required. 
        """
        return self._valueAttrName

    @valueAttrName.setter
    def valueAttrName(self, value):
        self._valueAttrName = value

    @property
    def authKeytab(self):
        """
            str: The optional parameter authKeytab specifies the file that contains the encrypted keys for the user that is specified by the authPrincipal parameter. The operator uses this keytab file to authenticate the user. The keytab file is generated by the administrator. You must specify this parameter to use Kerberos authentication.
        """
        return self._authKeytab

    @authKeytab.setter
    def authKeytab(self, value):
        self._authKeytab = value

    @property
    def authPrincipal(self):
        """
            str: The optional parameter authPrincipal specifies the Kerberos principal that you use for authentication. This value is set to the principal that is created for the IBM Streams instance owner. You must specify this parameter if you want to use Kerberos authentication. 
        """
        return self._authPrincipal

    @authPrincipal.setter
    def authPrincipal(self, value):
        self._authPrincipal = value


    @property
    def columnFamilyAttrName(self):
        """
            str: Name of the attribute on the input tuple containing the columnFamily. Cannot be used with staticColumnFmily. 
        """
        return self._columnFamilyAttrName

    @columnFamilyAttrName.setter
    def columnFamilyAttrName(self, value):
        self._columnFamilyAttrName = value

    @property
    def columnQualifierAttrName(self):
        """
            str: Name of the attribute on the input tuple containing the columnQualifier. Cannot be used with staticColumnQualifier.
        """
        return self._columnQualifierAttrName

    @columnQualifierAttrName.setter
    def columnQualifierAttrName(self, value):
        self._columnQualifierAttrName = value

    @property
    def hbaseSite(self):
        """
            str: The hbaseSite parameter specifies the path of hbase-site.xml file. This is the recommended way to specify the HBASE configuration. 
        """
        return self._hbaseSite

    @hbaseSite.setter
    def hbaseSite(self, value):
        self._hbaseSite = value



    @property
    def checkAttrName(self):
        """
            str: Name of the attribute specifying the tuple to check for before applying the Put or Delete. The type of the attribute is tuple with attributes columnFamily and columnQualifier, or a tuple with attributes columnFamily, columnQualifier, and value. In the first case, the Put or Delete will be allowed to proceed only when there is no entry for the row, columnFamily, columnQualifer combination. When the the type of the attribute given by checkAttrName contains an attribute value, the Put or Delete operation will only succeed when the entry specified the row, columnFamily, and columnQualifier has the given value. 
        """
        return self._checkAttrName

    @checkAttrName.setter
    def checkAttrName(self, value):
        self._checkAttrName = value
     

    @property
    def batchSize(self):
        """
            int: This parameter has been deprecated as of Streams 4.2.0. The enableBuffer parameter should be used instead. 
        """
        return self._batchSize

    @batchSize.setter
    def batchSize(self, value):
        self._batchSize = value

    @property
    def enableBuffer(self):
        """
            bool: When set to true, this parameter can improve the performance of the operator because tuples received by the operator will not be immediately forwarded to the HBase server. 
        """
        return self._enableBuffer

    @enableBuffer.setter
    def enableBuffer(self, value):
        self._enableBuffer = value


    @property
    def successAttr(self):
        """
            str: Attribute on the output port to be set to true if the check passes and the action is successful.
        """
        return self._successAttr

    @successAttr.setter
    def successAttr(self, value):
        self._successAttr = value


    @property
    def staticColumnFamily(self):
        """
            str: If this parameter is specified, it will be used as the columnFamily for all operations. (Compare to columnFamilyAttrName.) For HBASEScan, it can have cardinality greater than one. 
        """
        return self._staticColumnFamily

    @staticColumnFamily.setter
    def staticColumnFamily(self, value):
        self._staticColumnFamily = value

    @property
    def staticColumnQualifier(self):
        """
            str: If this parameter is specified, it will be used as the columnQualifier for all tuples. HBASEScan allows it to be specified multiple times. 
        """
        return self._staticColumnQualifier
    
    @staticColumnQualifier.setter
    def staticColumnQualifier(self, value):
        self._staticColumnQualifier = value


    @property
    def tableName(self):
        """
            str: Name of the HBASE table. It is an optional parameter but one of these parameters must be set in opeartor: 'tableName' or 'tableNameAttribute'. Cannot be used with 'tableNameAttribute'. If the table does not exist, the operator will throw an exception. 
        """
        return self._tableName

    @tableName.setter
    def tableName(self, value):
        self._tableName = value

    @property
    def tableNameAttribute(self):
        """
            str: Name of the attribute on the input tuple containing the tableName. Use this parameter to pass the table name to the operator via input port. Cannot be used with parameter 'tableName'. This is suitable for tables with the same schema. 
        """
        return self._tableNameAttribute

    @tableNameAttribute.setter
    def tableNameAttribute(self, value):
        self._tableNameAttribute = value

    @property
    def Timestamp(self):
        """
            int: This parameter specifies the timestamp in milliseconds (INT64). The timestamp allows for versioning of the cells. Everytime HBaes make a PUT on a table it set the timestamp. By default this is the current time in milliseconds, but you can set your own timestamp as well with this parametr. Cannot be used with TimestampAttrName.
        """
        return self._Timestamp

    @Timestamp.setter
    def Timestamp(self, value):
        self._Timestamp = value

    @property
    def TimestampAttrName(self):
        """
            str: Name of the attribute on the input tuple containing the timestamp in milliseconds. Cannot be used with Timestamp. 
         """
        return self._TimestampAttrName

    @TimestampAttrName.setter
    def TimestampAttrName(self, value):
        self._TimestampAttrName = value



    @property
    def vmArg(self):
        """
            str: The optional parameter vmArg parameter to specify additional JVM arguments that are required by the specific invocation of the operator. 
        """
        return self._vmArg

    @vmArg.setter
    def vmArg(self, value):
        self._vmArg = value

    def populate(self, topology, stream, schema, name, **options):
  
        if self.batchSize is not None:
            self.batchSize = streamsx.spl.types.int32(self.batchSize)
        if self.Timestamp is not None:
            self.Timestamp = streamsx.spl.types.int64(self.Timestamp)

        if self.enableBuffer is not None:
            if self.enableBuffer is True:
                self.enableBuffer = streamsx.spl.op.Expression.expression('true')
            else:
                self.enableBuffer = streamsx.spl.op.Expression.expression('false')

             
        # check streamsx.hbase version
        _add_toolkit_dependency(topology)

        if (_generate_hbase_site_xml(stream.topology, self.connection)):
            self.hbaseSite = 'etc/hbase-site.xml'
            _op = _HBASEPut(stream=stream, \
                        schema=self.schema, \
                        rowAttrName=self.rowAttrName, \
                        valueAttrName=self.valueAttrName, \
                        authKeytab=self.authKeytab, \
                        authPrincipal=self.authPrincipal, \
                        batchSize=self.batchSize, \
                        checkAttrName=self.checkAttrName, \
                        columnFamilyAttrName=self.columnFamilyAttrName, \
                        columnQualifierAttrName=self.columnQualifierAttrName, \
                        enableBuffer=self.enableBuffer, \
                        hbaseSite=self.hbaseSite, \
                        staticColumnFamily=self.staticColumnFamily, \
                        staticColumnQualifier=self.staticColumnQualifier, \
                        successAttr=self.successAttr, \
                        tableName=self.tableName, \
                        tableNameAttribute=self.tableNameAttribute, \
                        Timestamp=self.Timestamp, \
                        TimestampAttrName=self.TimestampAttrName, \
                        vmArg=self.vmArg, \
                        name=name)

            return _op.outputs[0]
        else:
            return None

class HBaseScan(streamsx.topology.composite.Source):
    """
    HBaseScan operator scans an HBase table. Like the FileSource operator, it has an optional input port.
    If no input port is specifed, then the operator scans the table according to the parameters that you specify, and sends the final punctuation.


    Example, puts tuples into HBase table 'streamsSample_lotr'::

        import streamsx.hbase as hbase

        output_schema = StreamSchema('tuple<rstring who, rstring infoType, rstring requestedDetail, rstring value, int32 numResults>')

        HBasePutParameters = {
          'columnFamilyAttrName' : 'infoType',
          'columnQualifierAttrName' : "requestedDetail",
          'HBasePutParameters' : "value" ,
         }       

        scanned_rows = inputStream.map(hbase.HBaseScan(tableName='streamsSample_lotr', rowAttrName='who', schema=output_schema, **HBasePutParameters))
 
        scanned_rows.print()
 
 

    Attributes
    ----------
    hbaseSite : dict|str
        The hbaseSite specifies the path of hbase-site.xml file. .
    schema : StreamSchema
        Output schema, defaults to CommonSchema.String
    options : kwargs
        The additional optional parameters as variable keyword arguments.
    """

 
    def __init__(self, tableName, connection=None, schema=CommonSchema.String, name=None, **options):
        self.name = name
        self.schema = schema
        self.connection = connection
        self.authKeytab = None
        self.authPrincipal = None
        self.channel = None
        self.endRow = None
        self.hbaseSite = None
        self.initDelay = None
        self.maxChannels = None
        self.maxThreads = None
        self.maxVersions = None
        self.minTimestamp = None
        self.outAttrName = None
        self.outputCountAttr = None
        self.rowPrefix = None
        self.startRow = None
        self.staticColumnFamily = None
        self.staticColumnQualifier = None
        self.tableName = tableName
        self.tableNameAttribute = None
        self.triggerCount = None
        self.vmArg = None
  

        if 'authKeytab' in options:
            self.authKeytab = options.get('authKeytab')
        if 'authPrincipal' in options:
            self.authPrincipal = options.get('authPrincipal')
        if 'channel' in options:
            self.channel = options.get('channel')
        if 'endRow' in options:
            self.endRow = options.get('endRow')
        if 'hbaseSite' in options:
            self.hbaseSite = options.get('hbaseSite')
        if 'initDelay' in options:
            self.initDelay = options.get('initDelay')
        if 'maxChannels' in options:
            self.maxChannels = options.get('maxChannels')
        if 'maxThreads' in options:
            self.maxThreads = options.get('maxThreads')
        if 'maxVersions' in options:
            self.maxVersions = options.get('maxVersions')
        if 'minTimestamp' in options:
            self.minTimestamp = options.get('minTimestamp')
        if 'maxVersions' in options:
            self.maxVersions = options.get('maxVersions')
        if 'outAttrName' in options:
            self.outAttrName = options.get('outAttrName')
        if 'outputCountAttr' in options:
            self.outputCountAttr = options.get('outputCountAttr')
        if 'rowPrefix' in options:
            self.rowPrefix = options.get('rowPrefix')
        if 'startRow' in options:
            self.startRow = options.get('startRow')
        if 'staticColumnFamily' in options:
            self.staticColumnFamily = options.get('staticColumnFamily')
        if 'staticColumnFamily' in options:
            self.staticColumnFamily = options.get('staticColumnFamily')
        if 'tableNameAttribute' in options:
            self.tableNameAttribute = options.get('tableNameAttribute')
        if 'tableName' in options:
            self.tableName = options.get('tableName')
        if 'triggerCount' in options:
            self.triggerCount = options.get('triggerCount')
        if 'vmArg' in options:
            self.vmArg = options.get('vmArg')
  
  
    @property
    def authKeytab(self):
        """
            str: The optional parameter authKeytab specifies the file that contains the encrypted keys for the user that is specified by the authPrincipal parameter. The operator uses this keytab file to authenticate the user. The keytab file is generated by the administrator. You must specify this parameter to use Kerberos authentication.
        """
        return self._authKeytab

    @authKeytab.setter
    def authKeytab(self, value):
        self._authKeytab = value

    @property
    def authPrincipal(self):
        """
            str: The optional parameter authPrincipal specifies the Kerberos principal that you use for authentication. This value is set to the principal that is created for the IBM Streams instance owner. You must specify this parameter if you want to use Kerberos authentication. 
        """
        return self._authPrincipal

    @authPrincipal.setter
    def authPrincipal(self, value):
        self._authPrincipal = value


    @property
    def channel(self):
        """
            int: If this operator is part of a parallel region, it shares the work of scanning with other operators in the region. To do this, set this parameter value by calling getChannel(). This parameter is required if the maximum number of channels has a value other than zero. 
        """
        return self._channel

    @channel.setter
    def channel(self, value):
        self._channel = value

    @property
    def endRow(self):
        """
            str: This parameter specifies the row to use to stop the scan. The row that you specify is excluded from the scan. 
        """
        return self._endRow

    @endRow.setter
    def endRow(self, value):
        self._endRow = value

    @property
    def hbaseSite(self):
        """
            str: The hbaseSite parameter specifies the path of hbase-site.xml file. This is the recommended way to specify the HBASE configuration. 
        """
        return self._hbaseSite

    @hbaseSite.setter
    def hbaseSite(self, value):
        self._hbaseSite = value

    @property
    def initDelay(self):
        """
            float: The parameter initDelay specifies the time to wait in seconds before the operator HBASEScan reads the first row. The default value is 0 . 
        """
        return self._initDelay

    @initDelay.setter
    def initDelay(self, value):
        self._initDelay = value
    @property

    def maxChannels(self):
        """
            int: If this operator is part of a parallel region, set this parameter value by calling getMaxChannels(). If the operator is in a parallel region, then the regions to be scanned are divided among the other copies of this operator in the other channels. If this parameter is set, you must also set the channel parameter. 
        """
        return self._maxChannels

    @maxChannels.setter
    def maxChannels(self, value):
        self._maxChannels = value

    @property
    def maxThreads(self):
        """
            int: Maximum number of threads to use to scan the table. Defaults to one. 
        """
        return self._maxThreads

    @maxThreads.setter
    def maxThreads(self, value):
        self._maxThreads = value




    @property
    def maxVersions(self):
        """
            int: This parameter specifies the minimum timestamp that is used for queries. The operator does not return any entries with a timestamp older than this value. Unless you specify the maxVersions parameter, the opertor returns only one entry in this time range.
        """
        return self._maxVersions

    @maxVersions.setter
    def maxVersions(self, value):
        self._maxVersions = value


    @property
    def minTimestamp(self):
        """
            int: This parameter specifies the minimum timestamp that is used for queries. The operator does not return any entries with a timestamp older than this value. Unless you specify the maxVersions parameter, the opertor returns only one entry in this time range.
        """
        return self._minTimestamp

    @minTimestamp.setter
    def minTimestamp(self, value):
        self._minTimestamp = value
     

    @property
    def outAttrName(self):
        """
            str: This parameter specifies the name of the attribute in which to put the value. It defaults to value. If the attribute is a tuple data type, the attribute names are used as columnQualifiers. If multiple families are included in the scan and they have the same columnQualifiers, there is no way of knowing which columnFamily was used to populate a tuple attribute. 
        """
        return self._outAttrName

    @outAttrName.setter
    def outAttrName(self, value):
        self._outAttrName = value

    @property
    def outputCountAttr(self):
        """
            str: This parameter specifies the output attribute in which to put the number of results that are found. When the result is a tuple, this parameter value is the number attributes that were populated in that tuple.
        """
        return self._outputCountAttr

    @outputCountAttr.setter
    def outputCountAttr(self, value):
        self._outputCountAttr = value


    @property
    def rowPrefix(self):
        """
            str: This parameter specifies that the scan only return rows that have this prefix. 
        """
        return self._rowPrefix

    @rowPrefix.setter
    def rowPrefix(self, value):
        self._rowPrefix = value

    @property
    def startRow(self):
        """
            str: This parameter specifies the row to use to start the scan. The row that you specify is included in the scan. 
        """
        return self._startRow

    @startRow.setter
    def startRow(self, value):
        self._startRow = value



    @property
    def staticColumnFamily(self):
        """
            str: If this parameter is specified, it will be used as the columnFamily for all operations. (Compare to columnFamilyAttrName.) For HBASEScan, it can have cardinality greater than one. 
        """
        return self._staticColumnFamily

    @staticColumnFamily.setter
    def staticColumnFamily(self, value):
        self._staticColumnFamily = value

    @property
    def staticColumnQualifier(self):
        """
            str: If this parameter is specified, it will be used as the columnQualifier for all tuples. HBASEScan allows it to be specified multiple times. 
        """
        return self._staticColumnQualifier
    
    @staticColumnQualifier.setter
    def staticColumnQualifier(self, value):
        self._staticColumnQualifier = value


    @property
    def tableName(self):
        """
            str: Name of the HBASE table. It is an optional parameter but one of these parameters must be set in opeartor: 'tableName' or 'tableNameAttribute'. Cannot be used with 'tableNameAttribute'. If the table does not exist, the operator will throw an exception. 
        """
        return self._tableName

    @tableName.setter
    def tableName(self, value):
        self._tableName = value

    @property
    def tableNameAttribute(self):
        """
            str: Name of the attribute on the input tuple containing the tableName. Use this parameter to pass the table name to the operator via input port. Cannot be used with parameter 'tableName'. This is suitable for tables with the same schema. 
        """
        return self._tableNameAttribute

    @tableNameAttribute.setter
    def tableNameAttribute(self, value):
        self._tableNameAttribute = value

    @property
    def triggerCount(self):
        """
            int: This parameter specifies the number of rows to process before triggering a drain. This parameter is valid only in a operator-driven consistent region. 
        """
        return self._triggerCount

    @triggerCount.setter
    def triggerCount(self, value):
        self._triggerCount = value


    @property
    def vmArg(self):
        """
            str: The optional parameter vmArg parameter to specify additional JVM arguments that are required by the specific invocation of the operator. 
        """
        return self._vmArg

    @vmArg.setter
    def vmArg(self, value):
        self._vmArg = value

    def populate(self, topology, stream, **options):
  
        if self.channel is not None:
            self.channel = streamsx.spl.types.int32(self.channel)
        if self.initDelay is not None:
            self.initDelay = streamsx.spl.types.float64(self.initDelay)
        if self.maxChannels is not None:
            self.maxChannels = streamsx.spl.types.int32(self.maxChannels)
        if self.maxThreads is not None:
            self.maxThreads = streamsx.spl.types.int32(self.maxThreads)
        if self.maxVersions is not None:
            self.maxVersions = streamsx.spl.types.int32(self.maxVersions)
        if self.minTimestamp is not None:
            self.minTimestamp = streamsx.spl.types.int64(self.minTimestamp)
        if self.triggerCount is not None:
            self.triggerCount = streamsx.spl.types.int64(self.triggerCount)
              
        # check streamsx.hbase version
        _add_toolkit_dependency(topology)

        if (_generate_hbase_site_xml(topology, self.connection)):
            self.hbaseSite = 'etc/hbase-site.xml'


            _op = _HBASEScan(topology=topology, \
                        schema=self.schema, \
                        authKeytab=self.authKeytab, \
                        authPrincipal=self.authPrincipal, \
                        channel=self.channel, \
                        endRow=self.endRow, \
                        hbaseSite=self.hbaseSite, \
                        initDelay=self.initDelay, \
                        maxChannels=self.maxChannels, \
                        maxThreads=self.maxThreads, \
                        maxVersions=self.maxVersions, \
                        minTimestamp=self.minTimestamp, \
                        outAttrName=self.outAttrName, \
                        outputCountAttr=self.outputCountAttr, \
                        rowPrefix=self.rowPrefix, \
                        startRow=self.startRow, \
                        staticColumnFamily=self.staticColumnFamily, \
                        staticColumnQualifier=self.staticColumnQualifier, \
                        tableName=self.tableName, \
                        tableNameAttribute=self.tableNameAttribute, \
                        triggerCount=self.triggerCount, \
                        vmArg=self.vmArg, \
                        name='HBaseScan')

            return _op.outputs[0]
        else:
            return None


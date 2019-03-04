# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import datetime
import os
from tempfile import gettempdir
import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.spl.types import rstring
from urllib.parse import urlparse


FileInfoSchema = StreamSchema('tuple<rstring fileName, uint64 fileSize>')
"""Structured schema of the file write response tuple. This schema is the output schema of the write method.

``'tuple<rstring fileName, uint64 fileSize>'``
"""

def _read_ae_service_credentials(credentials):
    hbase_uri = ""
    user = ""
    password = ""
    if isinstance(credentials, dict):
        # check for Analytics Engine service credentials
        if 'cluster' in credentials:
            user = credentials.get('cluster').get('user')
            password = credentials.get('cluster').get('password')
            hbase_uri = credentials.get('cluster').get('service_endpoints').get('webhbase')
        else:
            raise ValueError(credentials)
    else:
        raise TypeError(credentials)
    # construct expected format for hbase_uri: webhbase://host:port
    uri_parsed = urlparse(hbase_uri)
    hbase_uri = 'webhbase://'+uri_parsed.netloc
    return hbase_uri, user, password
   
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


def scan(topology, credentials, directory, pattern=None, init_delay=None, schema=CommonSchema.String, name=None):
    """Scans a Hadoop Distributed File System directory for new or modified files.

    Repeatedly scans a HBASE directory and writes the names of new or modified files that are found in the directory to the output stream.

    Args:
        topology(Topology): Topology to contain the returned stream.
        credentials(dict|file): The credentials of the IBM cloud Analytics Engine service in *JSON* or the path to the *configuration file* (``hbase-site.xml`` or ``core-site.xml``). If the *configuration file* is specified, then this file will be copied to the 'etc' directory of the application bundle.     
        directory(str): The directory to be scanned. Relative path is relative to the '/user/userid/' directory. 
        pattern(str): Limits the file names that are listed to the names that match the specified regular expression.
        init_delay(int|float|datetime.timedelta): The time to wait in seconds before the operator scans the directory for the first time. If not set, then the default value is 0.
        schema(Schema): Optional output stream schema. Default is ``CommonSchema.String``. Alternative a structured streams schema with a single attribute of type ``rstring`` is supported.  
        name(str): Source name in the Streams context, defaults to a generated name.

    Returns:
        Output Stream containing file names. Default output schema is ``CommonSchema.String``.
    """

    _op = _HBASScan(topology, directory=directory, pattern=pattern, schema=schema, name=name)
    if isinstance(credentials, dict):
        hbase_uri, user, password = _read_ae_service_credentials(credentials)
        _op.params['outAttrName'] = hbase_uri
        _op.params['outputCountAttr'] = user
        _op.params['minTimestamp'] = password
    else:
        # expect core-site.xml file in credentials param 
        topology.add_file_dependency(credentials, 'etc')
        _op.params['hbaseSite'] = 'etc'
    if init_delay is not None:
        _op.params['initDelay'] = streamsx.spl.types.float64(_check_time_param(init_delay, 'init_delay'))

    return _op.outputs[0]


def read(stream, credentials, schema=CommonSchema.String, name=None):
    """Reads files from a Hadoop Distributed File System.

    Filenames of file to be read are part of the input stream.

    Args:
        stream(Stream): Stream of tuples containing file names to be read. Supports ``CommonSchema.String`` as input. Alternative a structured streams schema with a single attribute of type ``rstring`` is supported.
        credentials(dict|file): The credentials of the IBM cloud Analytics Engine service in *JSON* or the path to the *configuration file* (``hbase-site.xml`` or ``core-site.xml``). If the *configuration file* is specified, then this file will be copied to the 'etc' directory of the application bundle.     
        schema(Schema): Output schema for the file content, defaults to ``CommonSchema.String``. Alternative a structured streams schema with a single attribute of type ``rstring`` or ``blob`` is supported.
        name(str): Name of the operator in the Streams context, defaults to a generated name.

    Returns:
        Output Stream for file content. Default output schema is ``CommonSchema.String`` (line per file).
    """

    _op = _HBASGet(stream, schema=schema, name=name)
    if isinstance(credentials, dict):
        hbase_uri, user, password = _read_ae_service_credentials(credentials)
        _op.params['outAttrName'] = hbase_uri
        _op.params['outputCountAttr'] = user
        _op.params['minTimestamp'] = password
    else:
        # expect core-site.xml file in credentials param 
        stream.topology.add_file_dependency(credentials, 'etc')
        _op.params['hbaseSite'] = 'etc'

    return _op.outputs[0]


def write(stream, credentials, file, time_per_file=None, tuples_per_file=None, bytes_per_file=None, name=None):
    """Writes files to a Hadoop Distributed File System.

    When writing to a file, that exists already on HBASE with the same name, then this file is overwritten.
    Per default the file is closed when window punctuation mark is received. Different close modes can be specified with the parameters: ``time_per_file``, ``tuples_per_file``, ``bytes_per_file``

    Example with input stream of type ``CommonSchema.String``::

        import streamsx.hbase as hbase
        
        s = topo.source(['Hello World!']).as_string()
        result = hbase.write(s, credentials=credentials, file='sample%FILENUM.txt')
        result.print()

    Args:
        stream(Stream): Stream of tuples containing the data to be written to files. Supports ``CommonSchema.String`` as input. Alternative a structured streams schema with a single attribute of type ``rstring`` or ``blob`` is supported.
        credentials(dict|file): The credentials of the IBM cloud Analytics Engine service in *JSON* or the path to the *configuration file* (``hbase-site.xml`` or ``core-site.xml``). If the *configuration file* is specified, then this file will be copied to the 'etc' directory of the application bundle.     
        file(str): Specifies the name of the file. The file parameter can optionally contain the following variables, which are evaluated at runtime to generate the file name:
         
          * %FILENUM The file number, which starts at 0 and counts up as a new file is created for writing.
         
          * %TIME The time when the file is created. The time format is yyyyMMdd_HHmmss.
          
 
    Returns:
        Output Stream with schema :py:const:`~streamsx.hbase.FileInfoSchema`.
    """
    
    # check bytes_per_file, time_per_file and tuples_per_file parameters
    if (time_per_file is not None and tuples_per_file is not None) or (tuples_per_file is not None and bytes_per_file is not None) or (time_per_file is not None and bytes_per_file is not None):
        raise ValueError("The parameters are mutually exclusive: bytes_per_file, time_per_file, tuples_per_file")

    _op = _HBASEPut(stream, file=file, schema=FileInfoSchema, name=name)
    if isinstance(credentials, dict):
        hbase_uri, user, password = _read_ae_service_credentials(credentials)
        _op.params['outAttrName'] = hbase_uri
        _op.params['outputCountAttr'] = user
        _op.params['minTimestamp'] = password
    else:
        # expect core-site.xml file in credentials param 
        stream.topology.add_file_dependency(credentials, 'etc')
        _op.params['hbaseSite'] = 'etc'

    if time_per_file is None and tuples_per_file is None and bytes_per_file is None:
        _op.params['closeOnPunct'] = _op.expression('true')
    if time_per_file is not None:
        _op.params['timePerFile'] = streamsx.spl.types.float64(_check_time_param(time_per_file, 'time_per_file'))
    if tuples_per_file is not None:
        _op.params['tuplesPerFile'] = streamsx.spl.types.int64(tuples_per_file)
    if bytes_per_file is not None:
        _op.params['bytesPerFile'] = streamsx.spl.types.int64(bytes_per_file)
    return _op.outputs[0]


# HBASEGet
# Required parameter: rowAttrName
# Optional parameters: authKeytab, authPrincipal, columnFamilyAttrName, columnQualifierAttrName, hbaseSite, maxVersions, 
# minTimestamp, outAttrName, outputCountAttr, staticColumnFamily, staticColumnQualifier, tableName, tableNameAttribute
class _HBASEGet(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, rowAttrName=None, authKeytab=None, authPrincipal=None, columnFamilyAttrName=None, columnQualifierAttrName=None, hbaseSite=None, maxVersions=None, minTimestamp=None, outAttrName=None, outputCountAttr=None, staticColumnFamily=None, staticColumnQualifier=None, libPath=None, tableName=None, tableNameAttribute=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hbase::HBASEGet"
        inputs=stream
        schemas=schema
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

# HBASEPut
# Required parameters: rowAttrName, valueAttrName
# Optional parameters: authKeytab, authPrincipal, batchSize, checkAttrName, columnFamilyAttrName, columnQualifierAttrName, 
# enableBuffer, hbaseSite, staticColumnFamily, staticColumnQualifier, successAttr, tableName, tableNameAttribute
class _HBASEPut(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, rowAttrName=None, valueAttrName=None, authKeytab=None, authPrincipal=None, batchSize=None, checkAttrName=None, columnFamilyAttrName=None, columnQualifierAttrName=None, enableBuffer=None, hbaseSite=None, staticColumnFamily=None, staticColumnQualifier=None, successAttr=None, tableName=None, tableNameAttribute=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hbase::HBASEPut"
        inputs=stream
        schemas=schema
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
        schemas=schema
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
    def __init__(self, stream, schema=None, rowAttrName=None, authKeytab=None, authPrincipal=None, columnFamilyAttrName=None, columnQualifierAttrName=None, hbaseSite=None,  increment=None, incrementAttrName=None, staticColumnFamily=None, staticColumnQualifier=None, tableName=None, tableNameAttribute=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hbase::HBASEIncrement"
        inputs=stream
        schemas=schema
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


# HBASEScan
# Optional parameter: authKeytab, authPrincipal, channel, endRow, hbaseSite, initDelay, maxChannels, maxThreads, maxVersions, minTimestamp, 
# outAttrName, outputCountAttr, rowPrefix, startRow, staticColumnFamily, staticColumnQualifier, tableName, tableNameAttribute, triggerCount
class _HBASEScan(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, authKeytab=None, authPrincipal=None, channel=None, endRow=None, hbaseSite=None, initDelay=None, maxChannels=None, maxThreads=None,  maxVersions=None, minTimestamp=None, outAttrName=None, outputCountAttr=None, rowPrefix=None, startRow=None, staticColumnFamily=None, staticColumnQualifier=None, libPath=None, tableName=None, tableNameAttribute=None, triggerCount=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hbase::HBASEScan"
        inputs=stream
        schemas=schema
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




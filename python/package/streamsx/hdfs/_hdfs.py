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
    hdfs_uri = ""
    user = ""
    password = ""
    if isinstance(credentials, dict):
        # check for Analytics Engine service credentials
        if 'cluster' in credentials:
            user = credentials.get('cluster').get('user')
            password = credentials.get('cluster').get('password')
            hdfs_uri = credentials.get('cluster').get('service_endpoints').get('webhdfs')
        else:
            raise ValueError(credentials)
    else:
        raise TypeError(credentials)
    # construct expected format for hdfs_uri: webhdfs://host:port
    uri_parsed = urlparse(hdfs_uri)
    hdfs_uri = 'webhdfs://'+uri_parsed.netloc
    return hdfs_uri, user, password
   
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

    Repeatedly scans a HDFS directory and writes the names of new or modified files that are found in the directory to the output stream.

    Args:
        topology(Topology): Topology to contain the returned stream.
        credentials(dict|file): The credentials of the IBM cloud Analytics Engine service in *JSON* or the path to the *configuration file* (``hdfs-site.xml`` or ``core-site.xml``). If the *configuration file* is specified, then this file will be copied to the 'etc' directory of the application bundle.     
        directory(str): The directory to be scanned. Relative path is relative to the '/user/userid/' directory. 
        pattern(str): Limits the file names that are listed to the names that match the specified regular expression.
        init_delay(int|float|datetime.timedelta): The time to wait in seconds before the operator scans the directory for the first time. If not set, then the default value is 0.
        schema(Schema): Optional output stream schema. Default is ``CommonSchema.String``. Alternative a structured streams schema with a single attribute of type ``rstring`` is supported.  
        name(str): Source name in the Streams context, defaults to a generated name.

    Returns:
        Output Stream containing file names. Default output schema is ``CommonSchema.String``.
    """

    _op = _HDFS2DirectoryScan(topology, directory=directory, pattern=pattern, schema=schema, name=name)
    if isinstance(credentials, dict):
        hdfs_uri, user, password = _read_ae_service_credentials(credentials)
        _op.params['hdfsUri'] = hdfs_uri
        _op.params['hdfsUser'] = user
        _op.params['hdfsPassword'] = password
    else:
        # expect core-site.xml file in credentials param 
        topology.add_file_dependency(credentials, 'etc')
        _op.params['configPath'] = 'etc'
    if init_delay is not None:
        _op.params['initDelay'] = streamsx.spl.types.float64(_check_time_param(init_delay, 'init_delay'))

    return _op.outputs[0]


def read(stream, credentials, schema=CommonSchema.String, name=None):
    """Reads files from a Hadoop Distributed File System.

    Filenames of file to be read are part of the input stream.

    Args:
        stream(Stream): Stream of tuples containing file names to be read. Supports ``CommonSchema.String`` as input. Alternative a structured streams schema with a single attribute of type ``rstring`` is supported.
        credentials(dict|file): The credentials of the IBM cloud Analytics Engine service in *JSON* or the path to the *configuration file* (``hdfs-site.xml`` or ``core-site.xml``). If the *configuration file* is specified, then this file will be copied to the 'etc' directory of the application bundle.     
        schema(Schema): Output schema for the file content, defaults to ``CommonSchema.String``. Alternative a structured streams schema with a single attribute of type ``rstring`` or ``blob`` is supported.
        name(str): Name of the operator in the Streams context, defaults to a generated name.

    Returns:
        Output Stream for file content. Default output schema is ``CommonSchema.String`` (line per file).
    """

    _op = _HDFS2FileSource(stream, schema=schema, name=name)
    if isinstance(credentials, dict):
        hdfs_uri, user, password = _read_ae_service_credentials(credentials)
        _op.params['hdfsUri'] = hdfs_uri
        _op.params['hdfsUser'] = user
        _op.params['hdfsPassword'] = password
    else:
        # expect core-site.xml file in credentials param 
        stream.topology.add_file_dependency(credentials, 'etc')
        _op.params['configPath'] = 'etc'

    return _op.outputs[0]


def write(stream, credentials, file, time_per_file=None, tuples_per_file=None, bytes_per_file=None, name=None):
    """Writes files to a Hadoop Distributed File System.

    When writing to a file, that exists already on HDFS with the same name, then this file is overwritten.
    Per default the file is closed when window punctuation mark is received. Different close modes can be specified with the parameters: ``time_per_file``, ``tuples_per_file``, ``bytes_per_file``

    Example with input stream of type ``CommonSchema.String``::

        import streamsx.hdfs as hdfs
        
        s = topo.source(['Hello World!']).as_string()
        result = hdfs.write(s, credentials=credentials, file='sample%FILENUM.txt')
        result.print()

    Args:
        stream(Stream): Stream of tuples containing the data to be written to files. Supports ``CommonSchema.String`` as input. Alternative a structured streams schema with a single attribute of type ``rstring`` or ``blob`` is supported.
        credentials(dict|file): The credentials of the IBM cloud Analytics Engine service in *JSON* or the path to the *configuration file* (``hdfs-site.xml`` or ``core-site.xml``). If the *configuration file* is specified, then this file will be copied to the 'etc' directory of the application bundle.     
        file(str): Specifies the name of the file. The file parameter can optionally contain the following variables, which are evaluated at runtime to generate the file name:
         
          * %FILENUM The file number, which starts at 0 and counts up as a new file is created for writing.
         
          * %TIME The time when the file is created. The time format is yyyyMMdd_HHmmss.
          
          Important: If the %FILENUM or %TIME specification is not included, the file is overwritten every time a new file is created.
        time_per_file(int|float|datetime.timedelta): Specifies the approximate time, in seconds, after which the current output file is closed and a new file is opened for writing. The ``bytes_per_file``, ``time_per_file`` and ``tuples_per_file`` parameters are mutually exclusive.
        tuples_per_file(int): The maximum number of tuples that can be received for each output file. When the specified number of tuples are received, the current output file is closed and a new file is opened for writing. The ``bytes_per_file``, ``time_per_file`` and ``tuples_per_file`` parameters are mutually exclusive. 
        bytes_per_file(int): Approximate size of the output file, in bytes. When the file size exceeds the specified number of bytes, the current output file is closed and a new file is opened for writing. The ``bytes_per_file``, ``time_per_file`` and ``tuples_per_file`` parameters are mutually exclusive.
        name(str): Sink name in the Streams context, defaults to a generated name.

    Returns:
        Output Stream with schema :py:const:`~streamsx.hdfs.FileInfoSchema`.
    """
    
    # check bytes_per_file, time_per_file and tuples_per_file parameters
    if (time_per_file is not None and tuples_per_file is not None) or (tuples_per_file is not None and bytes_per_file is not None) or (time_per_file is not None and bytes_per_file is not None):
        raise ValueError("The parameters are mutually exclusive: bytes_per_file, time_per_file, tuples_per_file")

    _op = _HDFS2FileSink(stream, file=file, schema=FileInfoSchema, name=name)
    if isinstance(credentials, dict):
        hdfs_uri, user, password = _read_ae_service_credentials(credentials)
        _op.params['hdfsUri'] = hdfs_uri
        _op.params['hdfsUser'] = user
        _op.params['hdfsPassword'] = password
    else:
        # expect core-site.xml file in credentials param 
        stream.topology.add_file_dependency(credentials, 'etc')
        _op.params['configPath'] = 'etc'

    if time_per_file is None and tuples_per_file is None and bytes_per_file is None:
        _op.params['closeOnPunct'] = _op.expression('true')
    if time_per_file is not None:
        _op.params['timePerFile'] = streamsx.spl.types.float64(_check_time_param(time_per_file, 'time_per_file'))
    if tuples_per_file is not None:
        _op.params['tuplesPerFile'] = streamsx.spl.types.int64(tuples_per_file)
    if bytes_per_file is not None:
        _op.params['bytesPerFile'] = streamsx.spl.types.int64(bytes_per_file)
    return _op.outputs[0]


class _HDFS2DirectoryScan(streamsx.spl.op.Source):
    def __init__(self, topology, schema, vmArg=None, authKeytab=None, authPrincipal=None, configPath=None, credFile=None, directory=None, hdfsPassword=None, hdfsUri=None, hdfsUser=None, initDelay=None, keyStorePassword=None, keyStorePath=None, libPath=None, pattern=None, policyFilePath=None, reconnectionBound=None, reconnectionInterval=None, reconnectionPolicy=None, sleepTime=None, strictMode=None, name=None):
        kind="com.ibm.streamsx.hdfs::HDFS2DirectoryScan"
        inputs=None
        schemas=schema
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if configPath is not None:
            params['configPath'] = configPath
        if credFile is not None:
            params['credFile'] = credFile
        if directory is not None:
            params['directory'] = directory
        if hdfsPassword is not None:
            params['hdfsPassword'] = hdfsPassword
        if hdfsUri is not None:
            params['hdfsUri'] = hdfsUri
        if hdfsUser is not None:
            params['hdfsUser'] = hdfsUser
        if initDelay is not None:
            params['initDelay'] = initDelay
        if keyStorePassword is not None:
            params['keyStorePassword'] = keyStorePassword
        if keyStorePath is not None:
            params['keyStorePath'] = keyStorePath
        if libPath is not None:
            params['libPath'] = libPath
        if pattern is not None:
            params['pattern'] = pattern
        if policyFilePath is not None:
            params['policyFilePath'] = policyFilePath
        if reconnectionBound is not None:
            params['reconnectionBound'] = reconnectionBound
        if reconnectionInterval is not None:
            params['reconnectionInterval'] = reconnectionInterval
        if reconnectionPolicy is not None:
            params['reconnectionPolicy'] = reconnectionPolicy
        if sleepTime is not None:
            params['sleepTime'] = sleepTime
        if strictMode is not None:
            params['strictMode'] = strictMode

        super(_HDFS2DirectoryScan, self).__init__(topology,kind,schemas,params,name)


class _HDFS2FileSource(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, authKeytab=None, authPrincipal=None, blockSize=None, configPath=None, credFile=None, encoding=None, file=None, hdfsPassword=None, hdfsUri=None, hdfsUser=None, initDelay=None, keyStorePassword=None, keyStorePath=None, libPath=None, policyFilePath=None, reconnectionBound=None, reconnectionInterval=None, reconnectionPolicy=None, vmArg=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hdfs::HDFS2FileSource"
        inputs=stream
        schemas=schema
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if blockSize is not None:
            params['blockSize'] = blockSize
        if configPath is not None:
            params['configPath'] = configPath
        if credFile is not None:
            params['credFile'] = credFile
        if encoding is not None:
            params['encoding'] = encoding
        if file is not None:
            params['file'] = file
        if hdfsPassword is not None:
            params['hdfsPassword'] = hdfsPassword
        if hdfsUri is not None:
            params['hdfsUri'] = hdfsUri
        if hdfsUser is not None:
            params['hdfsUser'] = hdfsUser
        if initDelay is not None:
            params['initDelay'] = initDelay
        if keyStorePassword is not None:
            params['keyStorePassword'] = keyStorePassword
        if keyStorePath is not None:
            params['keyStorePath'] = keyStorePath
        if libPath is not None:
            params['libPath'] = libPath
        if policyFilePath is not None:
            params['policyFilePath'] = policyFilePath
        if reconnectionBound is not None:
            params['reconnectionBound'] = reconnectionBound
        if reconnectionInterval is not None:
            params['reconnectionInterval'] = reconnectionInterval
        if reconnectionPolicy is not None:
            params['reconnectionPolicy'] = reconnectionPolicy

        super(_HDFS2FileSource, self).__init__(topology,kind,inputs,schema,params,name)


class _HDFS2FileSink(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, authKeytab=None, authPrincipal=None, bytesPerFile=None, closeOnPunct=None, configPath=None, credFile=None, encoding=None, file=None, fileAttributeName=None, hdfsPassword=None, hdfsUri=None, hdfsUser=None, keyStorePassword=None, keyStorePath=None, libPath=None, policyFilePath=None, reconnectionBound=None, reconnectionInterval=None, reconnectionPolicy=None, tempFile=None, timeFormat=None, timePerFile=None, tuplesPerFile=None, vmArg=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.hdfs::HDFS2FileSink"
        inputs=stream
        schemas=schema
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if authKeytab is not None:
            params['authKeytab'] = authKeytab
        if authPrincipal is not None:
            params['authPrincipal'] = authPrincipal
        if bytesPerFile is not None:
            params['bytesPerFile'] = bytesPerFile
        if closeOnPunct is not None:
            params['closeOnPunct'] = closeOnPunct
        if configPath is not None:
            params['configPath'] = configPath
        if credFile is not None:
            params['credFile'] = credFile
        if encoding is not None:
            params['encoding'] = encoding
        if file is not None:
            params['file'] = file
        if fileAttributeName is not None:
            params['fileAttributeName'] = fileAttributeName
        if hdfsPassword is not None:
            params['hdfsPassword'] = hdfsPassword
        if hdfsUri is not None:
            params['hdfsUri'] = hdfsUri
        if hdfsUser is not None:
            params['hdfsUser'] = hdfsUser
        if keyStorePassword is not None:
            params['keyStorePassword'] = keyStorePassword
        if keyStorePath is not None:
            params['keyStorePath'] = keyStorePath
        if libPath is not None:
            params['libPath'] = libPath
        if policyFilePath is not None:
            params['policyFilePath'] = policyFilePath
        if reconnectionBound is not None:
            params['reconnectionBound'] = reconnectionBound
        if reconnectionInterval is not None:
            params['reconnectionInterval'] = reconnectionInterval
        if reconnectionPolicy is not None:
            params['reconnectionPolicy'] = reconnectionPolicy
        if tempFile is not None:
            params['tempFile'] = tempFile
        if timeFormat is not None:
            params['timeFormat'] = timeFormat
        if timePerFile is not None:
            params['timePerFile'] = timePerFile
        if tuplesPerFile is not None:
            params['tuplesPerFile'] = tuplesPerFile

        super(_HDFS2FileSink, self).__init__(topology,kind,inputs,schema,params,name)




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

def _read_ae_service_credentials(credentials):
    hdfs_uri = ""
    user = ""
    password = ""
    if isinstance(credentials, dict):
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
   

def scan(topology, credentials, directory, init_delay=None, schema=CommonSchema.String, name=None):
    """Runs a SQL statement using DB2 client driver and JDBC database interface.

    The statement is called once for each input tuple received. Result sets that are produced by the statement are emitted as output stream tuples.

    Args:
        topology(Topology): Topology to contain the returned stream.
        credentials(dict): The credentials of the IBM cloud Analytics Engine service in JSON.     
        directory(str): The directory to be scanned.
        init_delay(int): The time to wait in seconds before the operator scans the directory for the first time. If not set, then the default value is 0.
        schema(Schema): Optional output stream schema. Default is ``CommonSchema.String``.      
        name(str): Source name in the Streams context, defaults to a generated name.

    Returns:
        Output Stream.
    """

    _op = _HDFS2DirectoryScan(topology, directory=directory, initDelay=init_delay, schema=schema, name=name)
    if isinstance(credentials, dict):
        hdfs_uri, user, password = _read_ae_service_credentials(credentials)
        _op.params['hdfsUri'] = hdfs_uri
        _op.params['hdfsUser'] = user
        _op.params['hdfsPassword'] = password
    else:
        # expect core-site.xml file in credentials param 
        topology.add_file_dependency(credentials, 'etc')
        _op.params['configPath'] = 'etc'


    return _op.outputs[0]

def read(stream, credentials, schema=None, name=None):
    return

def write(stream, credentials, schema=None, name=None):
    return


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




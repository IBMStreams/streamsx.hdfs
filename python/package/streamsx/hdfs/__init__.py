# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

"""
Overview
++++++++

Provides functions to access files on HDFS.

Use this package with the following services on IBM Cloud:

  * `Streaming Analytics <https://www.ibm.com/cloud/streaming-analytics>`_
  * `Analytics Engine <https://www.ibm.com/cloud/analytics-engine>`_


Credentials
+++++++++++

"Analytics Engine" credentials are defined using service credentials JSON.

The mandatory JSON elements are "user", "password" and "webhdfs"::

    {
        "cluster": {
            "password": "<PASSWORD>",
            "service_endpoints": {
                "webhdfs": "https://<HOST>:<PORT>/gateway/default/webhdfs/v1/"
            },
            "user": "<USER>"
        },
    }


Sample
++++++

A simple hello world example of a Streams application writing string messages to
a file to HDFS. Scan for created file on HDFS and read the content::

    from streamsx.topology.topology import *
    from streamsx.topology.schema import CommonSchema, StreamSchema
    from streamsx.topology.context import submit
    import streamsx.hdfs as hdfs

    topo = Topology('HDFSHelloWorld')

    to_hdfs = topo.source(['Hello', 'World!'])
    to_hdfs = to_hdfs.as_string()
   
    # Write a stream to HDFS
    hdfs.write(to_hdfs, '/sample/hw.txt')

    scanned = hdfs.scan(topo, directory='/sample')
    
    # read text file line by line
    r = hdfs.read(scanned)
    
    # print each line (tuple)
    r.print()

    submit('STREAMING_ANALYTICS_SERVICE', topo)

"""

__version__='0.1.0'

__all__ = ['scan', 'read', 'write']
from streamsx.hdfs._hdfs import scan, read, write

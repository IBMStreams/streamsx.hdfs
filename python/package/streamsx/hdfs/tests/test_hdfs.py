
import streamsx.hdfs as hdfs

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.toolkit as tk
import streamsx.rest as sr

import unittest
import datetime
import os
import json

##
## Test assumptions
##
## Streaming analytics service or Streams instance running
## IBM cloud Analytics Engine service credentials are located in a file referenced by environment variable ANALYTICS_ENGINE.
## The core-site.xml is referenced by HDFS_SITE_XML environment variable.
##
def toolkit_env_var():
    result = True
    try:
        os.environ['STREAMS_HDFS_TOOLKIT']
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
        os.environ['HDFS_SITE_XML']
    except KeyError: 
        result = False
    return result

def cloud_creds_env_var():
    result = True
    try:
        os.environ['ANALYTICS_ENGINE']
    except KeyError: 
        result = False
    return result

class TestParams(unittest.TestCase):

    @unittest.skipIf(cloud_creds_env_var() == False, "Missing ANALYTICS_ENGINE environment variable.")
    def test_cloud_creds(self):
        creds_file = os.environ['ANALYTICS_ENGINE']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology()
        hdfs.scan(topo, credentials, 'a_dir')

    @unittest.skipIf(site_xml_env_var() == False, "Missing HDFS_SITE_XML environment variable.")
    def test_xml_creds(self):
        xml_file = os.environ['HDFS_SITE_XML']
        topo = Topology()
        hdfs.scan(topo, credentials=xml_file, directory='a_dir')

    def test_bad_param(self):
        topo = Topology()
        # expect ValueError because credentials is neither a dict nor a file
        self.assertRaises(ValueError, hdfs.scan, topo, credentials='invalid', directory='any_dir')
        # expect ValueError because credentials is not expected JSON format
        invalid_creds = json.loads('{"user" : "user", "password" : "xx", "uri" : "xxx"}')
        self.assertRaises(ValueError, hdfs.scan, topo, credentials=invalid_creds, directory='any_dir')

class TestDistributed(unittest.TestCase):
    """ Test in local Streams instance with local toolkit from STREAMS_HDFS_TOOLKIT environment variable """

    @classmethod
    def setUpClass(self):
        print (str(self))

    def setUp(self):
        Tester.setup_distributed(self)
        self.hdfs_toolkit_location = os.environ['STREAMS_HDFS_TOOLKIT']
 
     # ------------------------------------
    @unittest.skipIf(site_xml_env_var() == False, "HDFS_SITE_XML environment variable.")
    def test_hdfs_config_path(self):
        hdfs_cfg_file = os.environ['HDFS_SITE_XML']

        topo = Topology('test_hdfs_config_path')

        if self.hdfs_toolkit_location is not None:
            tk.add_toolkit(topo, self.hdfs_toolkit_location)

        scanned_files = hdfs.scan(topo, credentials=hdfs_cfg_file, directory='testDirectory')
        scanned_files.print()

        tester = Tester(topo)
        tester.tuple_count(scanned_files, 1, exact=False)
        #tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

    # ------------------------------------
    @unittest.skipIf(cloud_creds_env_var() == False, "Missing ANALYTICS_ENGINE environment variable.")
    def test_hdfs_uri(self):
        ae_service_creds_file = os.environ['ANALYTICS_ENGINE']
        with open(ae_service_creds_file) as data_file:
            credentials = json.load(data_file)

        topo = Topology('test_hdfs_uri')

        if self.hdfs_toolkit_location is not None:
            tk.add_toolkit(topo, self.hdfs_toolkit_location)

        scanned_files = hdfs.scan(topo, credentials=credentials, directory='testDirectory')
        scanned_files.print()

        tester = Tester(topo)
        tester.tuple_count(scanned_files, 1, exact=False)
        #tester.run_for(60)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False     

        # Run the test
        tester.test(self.test_ctxtype, cfg, always_collect_logs=True)

    # ------------------------------------


class TestCloud(TestDistributed):
    """ Test in Streaming Analytics Service using local toolkit from STREAMS_HDFS_TOOLKIT environment variable """

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=False)
        self.hdfs_toolkit_location = os.environ['STREAMS_HDFS_TOOLKIT']



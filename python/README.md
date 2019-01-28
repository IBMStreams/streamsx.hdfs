# Python streamsx.hdfs package

This exposes SPL operators in the `com.ibm.streamsx.hdfs` toolkit as Python methods.

Package is organized using standard packaging to upload to PyPi.

The package is uploaded to PyPi in the standard way:
```
cd python/package
python setup.py sdist bdist_wheel upload -r pypi
```
Note: This is done using the `ibmstreams` account at pypi.org and requires `.pypirc` file containing the credentials in your home directory.

Package details: https://pypi.python.org/pypi/streamsx.hdfs

Documentation is using Sphinx and can be built locally using:
```
cd python/package/docs
make html
```
and viewed using
```
firefox python/package/docs/build/html/index.html
```

The documentation is also setup at `readthedocs.io`.

Documentation links:
* http://streamsxhdfs.readthedocs.io/en/pypackage

## Test

Package can be tested with TopologyTester using the [Streaming Analytics](https://www.ibm.com/cloud/streaming-analytics) service and [Analytics Engine](https://www.ibm.com/cloud/analytics-engine) service on IBM Cloud.

Analytics Engine service credentials are located in a file referenced by environment variable `ANALYTICS_ENGINE`.

Alternative the "core-site.xml" file can be specified for testing with the environment variable `HDFS_SITE_XML`.

```
cd python/package
python3 -u -m unittest streamsx.hdfs.tests.test_hdfs.TestDistributed

python3 -u -m unittest streamsx.hdfs.tests.test_hdfs.TestCloud
```

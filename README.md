# Python streamsx.hbase package

This exposes SPL operators in the `com.ibm.streamsx.hbase` toolkit as Python methods.

Package is organized using standard packaging to upload to PyPi.

The package is uploaded to PyPi in the standard way:
```
cd package
python setup.py sdist bdist_wheel upload -r pypi
```
Note: This is done using the `ibmstreams` account at pypi.org and requires `.pypirc` file containing the credentials in your home directory.

Package details: https://pypi.python.org/pypi/streamsx.hbase

Documentation is using Sphinx and can be built locally using:
```
cd package/docs
make html
```
and viewed using
```
firefox package/docs/build/html/index.html
```

The documentation is also setup at `readthedocs.io`.

Documentation links:
* http://streamsxhbase.readthedocs.io

## Test

Package can be tested with TopologyTester using the IBM Streams.

The host name and the port of hadoop server has to be specified for testing with the environment variable `HADOOP_HOST_PORT`.

For example:
```
export HADOOP_HOST_PORT=hdp264.fyre.ibm.com:8020
```
The package creates a HBase configiration file (hbase-site.xml) from a template.
And replaces the hadoop server name and the port with values from environment variable `HADOOP_HOST_PORT`.

Alternative the "hbase-site.xml" file can be specified for testing with the environment variable `HBASE_SITE_XML`.
For example:
```
export HBASE_SITE_XML=/usr/hdp/current/hbase-client/conf/hbase-site.xml
```

The location of hbase toolkit has to be specified for testing with the environment variable `STREAMS_HBASE_TOOLKIT`.

For example:
```
export STREAMS_HBASE_TOOLKIT=/opt/ibm/InfoSphere_Streams/4.3.0.0/toolkits/com.ibm.streamsx.hbase
```



### Test parameters only

This test does not require any Streams instance.

```
cd package
python3 -u -m unittest streamsx.hbase.tests.test_hbase.TestParams

```

### Test with local Streams instance

This test requires STREAMS_INSTALL set and a running Streams instance.

Required envionment variable for the `com.ibm.streamsx.hbase` toolkit  location: `STREAMS_HDFS_TOOLKIT`

```
cd package
python3 -u -m unittest streamsx.hbase.tests.test_hbase.TestDistributed
```



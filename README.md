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
The package creates a HBase configuration file (hbase-site.xml) from a template.

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

### Preparation


#### Installation of streamsx

```
pip install streamsx
pip install urllib3
pip install --upgrade pyOpenSSL
```

#### Create table

Before you begin with test, you have to create a test table on your HBASE database.

login as hbase user on your HBASE server.

```
hbase shell
....
create 'streamsSample_lotr','appearance','location'
```

The first test puts some rows into table via HBASEPut operator from streamsx.hbase toolkit.

In the next test the HBASEGet operator delivers the selected rows from table.

At the end the HBASEScan returns all rows for table.


### Test parameters only

This test does not require any Streams instance.

```
cd package
python3 -u -m unittest streamsx.hbase.tests.test_hbase.TestParams

```

### Test with local Streams instance

This test requires `STREAMS_INSTALL` set and a running Streams instance.

Required environment variable for the `com.ibm.streamsx.hbase` toolkit  location: `STREAMS_HBASE_TOOLKIT`

```
cd package
python3 -u -m unittest streamsx.hbase.tests.test_hbase.TestDistributed
```

or 

    ant test

For a quick test:

```
cd package
python3 -u -m unittest streamsx.hbase.tests.test_hbase.TestDistributedPut
```
### Test with local Streams instance and composite classes 
This test requires `STREAMS_INSTALL` set and a running Streams instance.

Required environment variable for the `com.ibm.streamsx.hbase` toolkit  location: `STREAMS_HBASE_TOOLKIT`
And `HADOOP_HOST_PORT` environment variable (hostname:port) to create a `hbase-site.xml` file.

- The standard `Beacon` operator creates 10 rows.
- The `HBasePut` puts the rows into test table 'streamsSample_lotr'
- The standard `Beacon` operator creates 10 rwos as query for HbsaeGet
- The `HBaseGet` gets the rows from test table 'streamsSample_lotr'
- The `HBaseScan` scans the test table 'streamsSample_lotr' and delivers the rows in output stream


```
cd package
python3 -u -m unittest streamsx.hbase.tests.test_hbase.TestCompositeClass
```
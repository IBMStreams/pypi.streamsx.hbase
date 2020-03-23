from setuptools import setup
import streamsx.hbase
setup(
  name = 'streamsx.hbase',
  packages = ['streamsx.hbase'],
  include_package_data=True,
  version = streamsx.hbase.__version__,
  description = 'HBASE integration for IBM Streams',
  long_description = open('DESC.txt').read(),
  author = 'IBM Streams @ github.com',
  author_email = 'ahmad.nouri@de.ibm.com',
  license='Apache License - Version 2.0',
  url = 'https://github.com/IBMStreams/streamsx.hbase',
  keywords = ['streams', 'ibmstreams', 'streaming', 'analytics', 'streaming-analytics', 'hbase'],
  classifiers = [
    'Development Status :: 5 - Production/Stable',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
  install_requires=['streamsx', 'streamsx.toolkits'],
  
  test_suite='nose.collector',
  tests_require=['nose']
)

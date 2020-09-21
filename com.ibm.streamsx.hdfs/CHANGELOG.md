# Changes
=======

## v5.3.0:
* [#129](https://github.com/IBMStreams/streamsx.hdfs/issues/129) The Vulnerability issue for 3rd party libraries:

- commons-httpclient-3.1.jar
- jackson-mapper-asl-1.9.13.jar
- guava-13.0.1.jar
- hadoop-common-3.1.0.jar
- hadoop-hdfs-3.1.0.jar

has been fixed. 

The maven pom.xml file has been upgraded to use hadoop client 3.3 
The following JAVA classes have been upgraded:
- com/ibm/streamsx/hdfs/client/webhdfs/JsonUtil.java
- com/ibm/streamsx/hdfs/client/webhdfs/WebHdfsFileSystem.java


## v5.2.3:
* [#128](https://github.com/IBMStreams/streamsx.hdfs/issues/128) The Vulnerability issue for 3rd party library commons-codec version 1.12 has been fixed. The maven pom.xml file has been upgraded to use commons-codec version 1.14 

## v5.2.2:
* [#122](https://github.com/IBMStreams/streamsx.hdfs/issues/122): The Vulnerability issue for 3rd party library commons-configuration2 version 2.5 has been fixed. The maven pom.xml file has been upgraded to use commons-configuration2 version 2.7.





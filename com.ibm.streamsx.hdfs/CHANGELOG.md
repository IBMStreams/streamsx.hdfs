# Changes
=======

## v5.3.2:
* [#133](https://github.com/IBMStreams/streamsx.hdfs/issues/140) slf4j jars updated (fix the log4j issue)

## v5.3.1:
* [#135](https://github.com/IBMStreams/streamsx.hdfs/issues/135) commons-io jar updated

## v5.3.1:
* [#133](https://github.com/IBMStreams/streamsx.hdfs/issues/133) guava jar updated
* [#132](https://github.com/IBMStreams/streamsx.hdfs/issues/132) Makefiles of samples updated


## v5.3.0:
* [#129](https://github.com/IBMStreams/streamsx.hdfs/issues/129) The Vulnerability issue for 3rd party libraries:

- commons-httpclient-3.1.jar
- jackson-mapper-asl-1.9.13.jar
- guava-13.0.1.jar
- hadoop-common-3.1.0.jar
- hadoop-hdfs-3.1.0.jar

has been fixed. 

The maven pom.xml file has been upgraded to use hadoop client 3.3.
```
commons-codec: 1.14            -->   commons-codec: 1.15
guava: 13.0.1                  -->   guava: 29.0-jre
hadoop-annotations: 3.1.0      -->   hadoop-annotations: 3.3.0
hadoop-auth: 3.1.0             -->   hadoop-auth: 3.3.0
hadoop-common: 3.1.0           -->   hadoop-common: 3.3.0
hadoop-hdfs: 3.1.0             -->   hadoop-hdfs: 3.3.0
hadoop-hdfs-client: 3.1.0      -->   hadoop-hdfs-client: 3.3.0
- jackson-core-asl: 1.9.13
- jackson-mapper-asl: 1.9.13
+ commons-compress: 1.20

httpcore: 4.4.11               -->   httpcore: 4.4.13
jackson-annotations: 2.10.2    -->   jackson-annotations: 2.11.2
jackson-core: 2.10.2           -->   jackson-core: 2.11.2
jackson-core-asl: 1.9.13       -->   jackson-databind: 2.11.2
jackson-databind: 2.10.2       -->   javax.servlet-api: 4.0.1
servlet-api: 2.5               -->   javax.servlet-api: 4.0.1
netty-all: 4.1.42.Final        -->   netty-all: 4.1.52.Final
woodstox-core: 5.0.3           -->   woodstox-core: 6.2.1

```

The following JAVA classes have been upgraded:
- com/ibm/streamsx/hdfs/client/webhdfs/JsonUtil.java
- com/ibm/streamsx/hdfs/client/webhdfs/WebHdfsFileSystem.java


## v5.2.3:
* [#128](https://github.com/IBMStreams/streamsx.hdfs/issues/128) The Vulnerability issue for 3rd party library commons-codec version 1.12 has been fixed. The maven pom.xml file has been upgraded to use commons-codec version 1.14 

## v5.2.2:
* [#122](https://github.com/IBMStreams/streamsx.hdfs/issues/122): The Vulnerability issue for 3rd party library commons-configuration2 version 2.5 has been fixed. The maven pom.xml file has been upgraded to use commons-configuration2 version 2.7.





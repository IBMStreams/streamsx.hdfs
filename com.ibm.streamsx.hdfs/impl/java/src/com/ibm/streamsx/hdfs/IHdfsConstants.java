/*******************************************************************************
* Copyright (C) 2017, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.hdfs;

public class IHdfsConstants {

    public static final String PARAM_TIME_PER_FILE = "timePerFile";
    public static final String PARAM_CLOSE_ON_PUNCT = "closeOnPunct";
    public static final String PARAM_TUPLES_PER_FILE = "tuplesPerFile";
    public static final String PARAM_BYTES_PER_FILE = "bytesPerFile";
    public static final String PARAM_FILE_NAME_ATTR = "fileAttributeName";
    public static final String PARAM_LOCAL_FILE_NAME_ATTR = "localFileAttrName";
    public static final String PARAM_HDFS_FILE_NAME_ATTR = "hdfsFileAttrName";
    public static final String PARAM_LOCAL_FILE_NAME = "localFile";
    public static final String PARAM_HDFS_FILE_NAME = "hdfsFile";
    public static final String PARAM_SLEEP_TIME = "sleepTime";
    public static final String PARAM_INITDELAY = "initDelay";
    public static final String PARAM_ENCODING = "encoding";
    public static final String FILE_VAR_PREFIX = "%";
    public static final String FILE_VAR_FILENUM = "%FILENUM";
    public static final String FILE_VAR_TIME = "%TIME";
    public static final String FILE_VAR_PELAUNCHNUM = "%PELAUNCHNUM";
    public static final String FILE_VAR_PEID = "%PEID";
    public static final String FILE_VAR_PROCID = "%PROCID";
    public static final String FILE_VAR_HOST = "%HOST";

    public final static String HDFS_PASSWORD = "password";
    public final static String AUTH_PRINCIPAL = "authPrincipal";
    public final static String AUTH_KEYTAB = "authKeytab";
    public final static String CRED_FILE = "credFile";

    public final static String FS_HDFS = "hdfs";
    public final static String FS_GPFS = "gpfs";
    public final static String FS_WEBHDFS = "webhdfs";
    public static final String KNOX_PASSWORD = "knox.password";
    public static final String KNOX_USER = "knox.user";
    public static final String KEYSTORE = "keystore_path";
    public static final String KEYSTORE_PASSWORD = "keystore_password";

    public static final String RECONNPOLICY_NORETRY = "NoRetry";
    public static final String RECONNPOLICY_BOUNDEDRETRY = "BoundedRetry";
    public static final String RECONNPOLICY_INFINITERETRY = "InfiniteRetry";
    public static final int RECONN_BOUND_DEFAULT = 5;
    public static final double RECONN_INTERVAL_DEFAULT = 10;

    public static final String DESC_HDFS_USER = "This parameter specifies the user ID to use when you connect to the HDFS file system. \\n"
            + "If this parameter is not specified, the operator uses the instance owner ID to connect to HDFS. \\n"
            + "When connecting to Hadoop instances on IBM Analytics Engine, this parameter must be specified otherwise the connection will be unsuccessful. \\n"
            + "When you use Kerberos authentication, the operator authenticates with the Hadoop file system as the instance owner by using the \\n"
            + "values that are specified in the **authPrincipal** and **authKeytab** parameters.  After successful authentication, the \\n"
            + "operator uses the user ID that is specified by the **hdfsUser** parameter to perform all other operations on the file system.";

    public static final String DESC_HDFS_URL = "This parameter specifies the uniform resource identifier (URI) that you can use to connect to \\n"
            + "the HDFS file system.  The URI has the following format:\\n"
            + "* To access HDFS locally or remotely, use `hdfs://hdfshost:hdfsport` \\n"
            + "* To access GPFS locally, use `gpfs:///`. \\n"
            + "* To access GPFS remotely, use `webhdfs://hdfshost:webhdfsport`. \\n"
            + "* To access HDFS via a web connection for HDFS deployed on IBM Analytics Engine, use `webhdfs://webhdfshost:webhdfsport`. \\n\\n"
            + "If this parameter is not specified, the operator expects that the HDFS URI is specified as the `fs.defaultFS` or "
            + "`fs.default.name` property in the `core-site.xml` HDFS configuration file.  The operator expects the `core-site.xml` \\n"
            + "file to be in `$HADOOP_HOME/../hadoop-conf` or `$HADOOP_HOME/etc/hadoop`  or in the directory specified by the **configPath** parameter. \\n"
            + "**Note:** For connections to HDFS on IBM Analytics Engine, the `$HADOOP_HOME` environment variable is not supported and so either  **hdfsUri** or **configPath**  must be specified.";

    public static final String DESC_INIT_DELAY = "This parameter specifies the time to wait in seconds before the operator reads the first file. \\n"
            + "The default value is `0`.";

    public static final String DESC_REC_POLICY = "This optional parameter specifies the policy that is used by the operator to handle HDFS connection failures. \\n"
            + "The valid values are: `NoRetry`, `InfiniteRetry`, and `BoundedRetry`. The default value is `BoundedRetry`. \\n"
            + "If `NoRetry` is specified and a HDFS connection failure occurs, the operator does not try to connect to the HDFS again. \\n"
            + "The operator shuts down at startup time if the initial connection attempt fails. \\n"
            + "If `BoundedRetry` is specified and a HDFS connection failure occurs, the operator tries to connect to the HDFS again up to a maximum number of times. \\n"
            + "The maximum number of connection attempts is specified in the **reconnectionBound** parameter.  The sequence of connection attempts occurs at startup time. \\n"
            + "If a connection does not exist, the sequence of connection attempts also occurs before each operator is run. \\n"
            + "If `InfiniteRetry` is specified, the operator continues to try and connect indefinitely until a connection is made. \\n"
            + "This behavior blocks all other operator operations while a connection is not successful. \\n"
            + "For example, if an incorrect connection password is specified in the connection configuration document, the operator remains in an infinite startup loop until a shutdown is requested.";

    public static final String DESC_REC_INTERVAL = "This optional parameter specifies the amount of time (in seconds) that the operator waits between successive connection attempts. \\n"
            + "It is used only when the **reconnectionPolicy** parameter is set to `BoundedRetry` or `InfiniteRetry`; othewise, it is ignored.  The default value is `10`.";

    public static final String DESC_REC_BOUND = "This optional parameter specifies the number of successive connection attempts that occur when a connection fails or a disconnect occurs. \\n"
            + "It is used only when the **reconnectionPolicy** parameter is set to `BoundedRetry`; otherwise, it is ignored. The default value is `5`.";

    public static final String DESC_PRINCIPAL = "This parameter specifies the Kerberos principal that you use for authentication. \\n"
            + "This value is set to the principal that is created for the IBM Streams instance owner. \\n"
            + "You must specify this parameter if you want to use Kerberos authentication.";

    public static final String DESC_AUTH_KEY = "This parameter specifies the file that contains the encrypted keys for the user that is specified by the **authPrincipal** parameter. \\n"
            + "The operator uses this keytab file to authenticate the user. \\n"
            + "The keytab file is generated by the administrator.  You must specify this parameter to use Kerberos authentication.";

    public static final String DESC_CRED_FILE = "This parameter specifies a file that contains login credentials. The credentials are used to "
            + "connect to GPFS remotely by using the `webhdfs://hdfshost:webhdfsport` schema.  The credentials file must contain information "
            + "about how to authenticate with IBM Analytics Engine when using the webhdfs schema. \\n"
            + "For example, the file must contain the user name and password for an IBM Analytics Engine user. \\n"
            + "When connecting to HDFS instances deployed on IBM Analytics Engine, \\n"
            + "the credentials are provided using the **hdfsUser** and **hdfsPassword** parameters.";

    public static final String DESC_CONFIG_PATH = "This parameter specifies the path to the directory that contains the `core-site.xml` file, which is an HDFS\\n"
            + "configuration file. If this parameter is not specified, by default the operator looks for the `core-site.xml` file in the following locations:\\n"
            + "* `$HADOOP_HOME/etc/hadoop`\\n" + "* `$HADOOP_HOME/conf`\\n"
            + "* `$HADOOP_HOME/lib` \\n" + "* `$HADOOP_HOME/`\\n"
            + "**Note:** For connections to Hadoop instances deployed on IBM Analytics Engine, the `$HADOOP_HOME` environment variable is not supported and should not be used.";

    public static final String DESC_POLICY_FILE_PATH = "This optional parameter is relevant when connecting to IBM Analytics Engine on IBM Cloud. \\n"
            + "It specifies the path to the directory that contains the Java Cryptography Extension policy files (US_export_policy.jar and local_policy.jar). \\n"
            + "The policy files enable the Java operators to use encryption with key sizes beyond the limits specified by the JDK. \\n"
            + "See the section on 'Policy file configuration' in the main page of this toolkit's documentation for information on how to configure the policy files. \\n"
            + "If this parameter is omitted the JVM default policy files will be used. When specified, this parameter takes precedence over the JVM default policy files. \\n\\n"
            + "**Note:** This parameter changes a JVM property. If you set this property, be sure it is set to the same value in all HDFS operators that are in the same PE. \\n"
            + "The location of the policy file directory can be absolute path on the file system or a path that is relative to the application directory.";

    public static final String DESC_LIB_PATH = "This optional parameter specifies the absolute path to the directory that contains the Hadoop library files. \\n"
            + "If this parameter is omitted and `$HADOOP_HOME` is not set, the apache hadoop specific libraries within the `impl/lib/ext` folder of the toolkit will be used. \\n"
            + "When specified, this parameter takes precedence over the `$HADOOP_HOME` environment variable and the libraries within the folder indicated by `$HADOOP_HOME` will not be used.";

    public static final String DESC_HDFS_PASSWORD = "This parameter specifies the password to use when you connecting to a Hadoop instance deployed on IBM Analytics Engine. \\n"
            + "If this parameter is not specified, attempts to connect to a Hadoop instance deployed on IBM Analytics Engine will cause an exception.";

    public static final String DESC_KEY_STOR_PATH = "This optional parameter is only supported when connecting to a Hadoop instance deployed on IBM Analytics Engine. \\n"
            + "It specifies the path to the keystore file, which is in PEM format. The keystore file is used when making a secure connection to the HDFS server and must contain the public certificate of the HDFS server that will be connected to. \\n"
            + "**Note: If this parameter is omitted, invalid certificates for secure connections will be accepted.**  If the keystore file does not exist, or if the certificate it contains is invalid, the operator terminates.. \\n"
            + "The location of the keystore file can be absolute path on the filesystem or a path that is relative to the application directory. \\n"
            + "See the section on 'SSL Configuration' in the main page of this toolkit's documentation for information on how to configure the keystore. \\n"
            + "The location of the keystore file can be absolute path on the filesystem or a path that is relative to the application directory.";

    public static final String DESC_KEY_STOR_PASSWORD = "This optional parameter is only supported when connecting to a Hadoop instance deployed on IBM Analytics Engine. \\n"
            + "It specifies the password for the keystore file. This attribute is specified when the **keyStore** attribute is specified and the keystore file is protected by a password. \\n"
            + "If the keyStorePassword is invalid the operator terminates.";

    public static final String DESC_ENCODING = "This optional parameter specifies the encoding to use when reading files. The default value is `UTF-8`.";

    public static final String DESC_BLOCK_SIZE = "This parameter specifies the maximum number of bytes to be read at one time when reading a file into binary mode (ie, into a blob); thus, it is the maximum size of the blobs on the output stream. The parameter is optional, and defaults to `4096`.";

    public static final String DESC_SOURCE_FILE = "This parameter specifies the name of the file that the operator opens and reads. \\n"
            + "This parameter must be specified when the optional input port is not configured. \\n"
            + "If the optional input port is used and the file name is specified, the operator generates an error.";

    public static final String DESC_SINK_FILE_ATTR = "If set, this points to an attribute containing the filename.  The operator will close a file when value of this attribute changes. \\n"
            + "If the string contains substitutions, the check for a change happens before substituations, and the filename contains the substitutions based on the first tuple.";

    public static final String DESC_SINK_FILE = "This parameter specifies the name of the file that the operator writes to. \\n"
            + "The **file** parameter can optionally contain the following variables, which the operator evaluates at runtime to generate the file name:\\n"
            + "* %HOST         The host that is running the processing element (PE) of this operator. \\n"
            + "* %FILENUM        The file number, which starts at 0 and counts up as a new file is created for writing. \\n"
            + "* %PROCID        The process ID of the processing element. \\n"
            + "* %PEID         The processing element ID. \\n"
            + "* %PELAUNCHNUM    The PE launch count. \\n"
            + "* %TIME         The time when the file is created.  If the **timeFormat** parameter is not specified, the default time format is `yyyyMMdd_HHmmss`. \\n\\n"
            +

            "For example, if you specify a **file** parameter of `myFile%FILENUM%TIME.txt`, and the first three files are created in the afternoon on November 30, 2014, \\n"
            + "the file names are `myFile020141130_132443.txt`, `myfile120141130_132443.txt`, and `myFile220141130_132443.txt`. \\n\\n"
            +

            "**Important:** If the %FILENUM specification is not included, the file is overwritten every time a new file is created. \\n";

    public static final String DESC_SINK_TEMP_FILE = "This parameter specifies the name of the file that the operator writes to. \\n"
            + "When the file is closed the file is renamed to the final filename defined by the **file** parameter or **fileAttributeName** parameter. \\n"
            + "The **tempFile** parameter can optionally contain the following variables, which the operator evaluates at runtime to generate the file name:\\n"
            + "* %HOST         The host that is running the processing element (PE) of this operator. \\n"
            + "* %PROCID        The process ID of the processing element. \\n"
            + "* %PEID         The processing element ID. \\n"
            + "* %PELAUNCHNUM    The PE launch count. \\n"
            + "* %TIME         The time when the file is created.  If the **timeFormat** parameter is not specified, the default time format is `yyyyMMdd_HHmmss`. \\n"
            + "**Important:** This parameter must not be used in a consistent region. \\n";

    public static final String DESC_SINK_TIME_FORMAT = "This parameter specifies the time format to use when the **file** parameter value contains `%TIME`. \\n"
            + "The parameter value must contain conversion specifications that are supported by the java.text.SimpleDateFormat. \\n"
            + "The default format is `yyyyMMdd_HHmmss`.";

    public static final String DESC_SINK_BYTES_PER_FILE = "This parameter specifies the approximate size of the output file, in bytes. \\n"
            + "When the file size exceeds the specified number of bytes, the current output file is closed and a new file is opened. \\n"
            + "The **bytesPerFile**, **timePerFile**, and **tuplesPerFile** parameters are mutually exclusive; you can specify only one of these parameters at a time.";

    public static final String DESC_SINK_TUPELS_PER_FILE = "This parameter specifies the maximum number of tuples that can be received for each output file. \\n"
            + "When the specified number of tuples are received, the current output file is closed and a new file is opened for writing. \\n"
            + "The **bytesPerFile**, **timePerFile**, and **tuplesPerFile** parameters are mutually exclusive; you can specify only one of these parameters at a time.";

    public static final String DESC_SINK_TIME_PER_FILE = "This parameter specifies the approximate time, in seconds, after which the current output file is closed and a new file is opened for writing. \\n"
            + "The **bytesPerFile**, **timePerFile**, and **tuplesPerFile** parameters are mutually exclusive; you can specify only one of these parameters.";

    public static final String DESC_SINK_CLOSE_ON_PUNCT = "This parameter specifies whether the operator closes the current output file and creates a new file when a punctuation marker is received. \\n"
            + "The default value is `false`.";

    public static final String DESC_HDFS_FILE_SOURCE = "The `HDFS2FileSource` operator reads files from a Hadoop Distributed File System (HDFS)\\n\\n"
            + "The operator opens a file on HDFS and sends out its contents in tuple format on its output port. \\n\\n"
            + "If the optional input port is not specified, the operator reads the HDFS file that is specified in the **file** parameter and \\n"
            + "provides the file contents on the output port.  If the optional input port is configured, the operator reads the files that are \\n"
            + "named by the attribute in the tuples that arrive on its input port and places a punctuation marker between each file. \\n\\n"

            + " # Behavior in a consistent region \\n"

            + "The `HDFS2FileSource` operator can participate in a consistent region. \\n"
            + "The operator can be at the start of a consistent region if there is no input port. \\n"
            + "The operator supports periodic and operator-driven consistent region policies. \\n"
            + "If the consistent region policy is set as operator driven, the operator initiates a drain after a file is fully read. \\n"
            + "If the consistent region policy is set as periodic, the operator respects the period setting and establishes consistent states accordingly. \\n"
            + "This means that multiple consistent states can be established before a file is fully read. \\n\\n"

            + "At checkpoint, the operator saves the current file name and file cursor location. \\n"
            + "If the operator does not have an input port, upon application failures, the operator resets "
            + "the file cursor back to the checkpointed location, and starts replaying tuples from the cursor location. \\n"
            + "If the operator has an input port and is in a consistent region, the operator relies on its upstream operators "
            + "to properly reply the filenames for it to re-read the files from the beginning. \\n\\n"

            + "# Exceptions\\n\\n"

            + "The `HDFS2FileSource` operator terminates in the following cases:\\n"
            + "* The operator cannot connect to HDFS. \\n"
            + "* The file cannot be opened. \\n"
            + "* The file does not exist. \\n"
            + "* The file becomes unreadable. \\n"
            + "* A tuple cannot be created from the file contents (such as a problem with the file format). \\n\\n"
            + "+ Examples\\n\\n"

            + "This example uses the `HDFS2DirectoryScan` operator to scan the HDFS directory every two seconds and the `HDFS2FileSource`\\n"
            + "operator to read the files that are output by the `HDFS2DirectoryScan` operator. \\n\\n"

            + "//// HDFS2DirectoryScan operator scans /user/myser/ directory from HDFS every 2.0 seconds\\n\\n"
            + "    (stream<rstring filename>; Files) as HDFS2DirectoryScan_1 = HDFS2DirectoryScan(){\\n"
            + "        param\\n"
            + "            directory     : \\\"/user/myuser/\\\"; \\n"
            + "            hdfsUri: \\\"hdfs : //hdfsServer:1883\\\"; \\n"
            + "            hdfsUser: \\\"streamsadmin\\\"; \\n"
            + "            hdfsPassword: \\\"Password\\\"; \\n"
            + "            sleepTime     : 2.0; \\n" 
            + "    }\\n\\n"
            
            + "    // HDFS2FileSource operator reads from files discovered by HDFS2DirectoryScan operator\\n"
            + "    //If the **keyStorePath** and **keyStorePassword** are omitted, the operator will accept all certificates as valid\\n"
            + "    (stream<rstring data> FileContent) as HDFS2FileSource_2 =    HDFS2FileSource(Files){\\n"
            + "         param\\n"
            + "            hdfsUri: \\\"hdfs://hdfsSever:8020\\\"; \\n"
            + "            hdfsUser: \\\"streamsadmin\\\"; \\n"
            + "           hdfsPassword: \\\"Password\\\"; \\n" 
            + "    }\\n\\n" 
            
            + "The following example shows the operator configured to access a HDFS instance on IBM Analytics Engine to read a file specified by the *file* parameter. \\n"
            
            + "The **hdfsUser** and **hdfsPassword** are the username and password that have access to the Hadoop instance. \\n\\n"
            
            + "    stream<rstring data> FileContent) as HDFS2FileSource_2 = HDFS2FileSource(){\\n"
            + "        param\\n"
            + "            hdfsUri: \\\"webhdfs://server_host_name:port\\\"; \\n"
            + "            file   : \\\"/user/streamsadmin/myfile.txt\\\"; \\n"
            + "            hdfsUser: \\\"streamsadmin\\\"; \\n"
            + "            hdfsPassword: \\\"Password\\\"; \\n"
            + "            keyStorePassword: \\\"storepass\\\"; \\n"
            + "            keyStorePath: \\\"etc/store.jks\\\"; \\n"
            + "    }\\n";

    public static final String DESC_HDFS_FILE_SOURCE_INPUT = "The `HDFS2FileSource` operator has one optional input port. If an input port is specified, the operator expects\\n"
            + "an input tuple with a single attribute of type rstring. The input tuples contain the file names that the operator opens for reading. \\n"
            + "The input port is non-mutating. \\n";

    public static final String DESC_HDFS_FILE_SOURCE_OUTPUT = "The `HDFS2FileSource` operator has one output port.  The tuples on the output port contain the data that is read from the files. \\n"
            + "The operator supports two modes of reading.  To read a file line-by-line, the expected output schema of the output port is tuple<rstring line> or tuple<ustring line>. \\n"
            + "To read a file as binary, the expected output schema of the output port is tuple<blob data>.  Use the blockSize parameter to control how much data to retrieve on each read. \\n"
            + "The operator includes a punctuation marker at the conclusion of each file. The output port is mutating.";

    public static final String DESC_HDFS_FILE_SINK = "The `HDFS2FileSink` operator writes files to a Hadoop Distributed File System. \\n\\n"
            + "The `HDFS2FileSink` operator is similar to the `FileSink` operator. \\n"
            + "This operator writes tuples that arrive on its input port to the output file that is named by the **file** parameter. \\n"
            + "You can optionally control whether the operator closes the current output file and creates a new file for writing based on the size \\n"
            + "of the file in bytes, the number of tuples that are written to the file, or the time in seconds that the file is open for writing, \\n\\n"
            + "or when the operator receives a punctuation marker. \\n" 
            
            + "# Behavior in a consistent region \\n\\n" 
            
            + "The `HDFS2FileSink` operator can participate in a consistent region, however this is not supported when connecting to IBM Analytics Engine on IBM Cloud. \\n"
            + "The operator can be part of a consistent region, but cannot be at the start of a consistent region. \\n"
            + "The operator guarantees that tuples are written to a file in HDFS at least once, \\n"
            + "but duplicated tuples can be written to the file if application failure occurs. \\n\\n"
            +

            "For the operator to support consistent region, the Hadoop Distributed File System must be configured with file append \\n"
            + "enabled. For information about how to properly enable this feature, refer to the documentation of your Hadoop distribution. \\n\\n"
            +

            "On drain, the operator flushes its internal buffer to the file. \\n"
            + "On checkpoint, the operator stores the current file name, file size, tuple count, and file number to the checkpoint. \\n"
            + "On reset, the operator closes the current file, and opens the file from checkpoint. \\n"
            + "File states like file size and tuple count are reset to the file. \\n"
            + "The file is opened in append mode, and data is written to the end of the file. \\n\\n"
            +

            "# Exceptions \\n\\n" +

            "The `HDFS2FileSink` operator terminates in the following cases: \\n"
            + "* The operator cannot connect to HDFS. \\n"
            + "* The file cannot be written. \\n\\n" +

            "+ Examples \\n\\n" +

            "This is a basic example using the `HDFS2FileSink` operator to write output to a Hadoop filesystem deployed on IBM Cloud (IBM Analytics Engine). \\n\\n"
            + "     () as Sink= HDFS2FileSink(Input){ \\n"
            + "          param \\n"
            + "             file          : \\\"/user/clsadmin/myfile.txt\\\"; \\n"
            + "             hdfsUri       : \\\"webhdfs://server_host_name:port\\\"; \\n"
            + "             hdfsUser      : \\\"clsadmin\\\"; \\n"
            + "             hdfsPassword  : \\\"IAEPassword\\\"; \\n"
            + "     } \\n\\n\\n" +

            "This example uses the `HDFS2FileSink` operator to write tuples to output files that have names like `output0.txt`. SSL certificate validation is enabled. \\n\\n"
            +

            "     stream<PersonSchema> In = FileSource(){ \\n"
            + "         param \\n"
            + "             file          : \\\"Input.txt\\\"; \\n"
            + "     } \\n\\n\\n" +

            "     stream<rstring PersonSchemString> SingleStringIn = Functor(In){ \\n"
            + "        output \\n"
            + "            SingleStringIn : PersonSchemString =(rstring) In ; \\n"
            + "     } \\n\\n\\n" +

            "     () as txtSink = HDFS2FileSink(SingleStringIn){ \\n"
            + "          param \\n"
            + "             file             : \\\"output%FILENUM.txt\\\"; \\n"
            + "             bytesPerFile     : (int64)(16*1024); \\n"
            + "             hdfsUri          : \\\"webhdfs://server_host_name:port\\\"; \\n"
            + "             hdfsUser         : \\\"clsadmin\\\"; \\n"
            + "             hdfsPassword     : \\\"IAEPassword\\\"; \\n"
            + "             keyStorePassword : \\\"storepass\\\"; \\n"
            + "             keyStorePath     : \\\"etc/store.jks\\\"; \\n"
            + "     }";

    public static final String DESC_HDFS_FILE_SINK_INPUT = "The `HDFS2FileSink` operator has one input port, which writes the contents of the input stream to the file that you specified. \\n"
            + "The input port is non-mutating, and its punctuation mode is `Oblivious`.  The HDFS2FileSink supports writing data into HDFS in two formats. \\n"
            + "For line format, the schema of the input port is tuple<rstring line> or tuple<ustring line>, which specifies a single rstring or ustring attribute that represents \\n"
            + "a line to be written to the file. For binary format, the schema of the input port is tuple<blob data>, which specifies a block of data to be written to the file.";

    public static final String DESC_HDFS_FILE_SINK_OUTPUT = "The `HDFS2FileSink` operator is configurable with an optional output port. \\n"
            + "The output port is non-mutating and its punctuation mode is `Free`. \\n"
            + "The schema of the output port is <string fileName, uint64 fileSize>, which specifies the name and size of files that are written to HDFS.";

    public static final String DESC_HDFS_DIR_SCAN = "The **HDFS2DirectoryScan** operator scans a Hadoop Distributed File System directory for new or modified files. \\n"
            + " \\n"
            + "The `HDFS2DirectoryScan` is similar to the `DirectoryScan` operator. \\n"
            + "The `HDFS2DirectoryScan` operator repeatedly scans an HDFS directory and writes the names of new or modified files \\n"
            + "that are found in the directory to the output port. The operator sleeps between scans. \\n\\n"

            + "# Behavior in a consistent region \\n\\n"

            + "The `HDFS2DirectoryScan` operator can participate in a consistent region. \\n"
            + "The operator can be at the start of a consistent region if there is no input port. \\n"
            + "The operator supports periodic and operator-driven consistent region policies. \\n\\n"

            + "If consistent region policy is set as operator driven, the operator initiates a drain after each tuple is submitted. \\n"
            + "This allows for a consistent state to be established after a file is fully processed. \\n"
            + "If consistent region policy is set as periodic, the operator respects the period setting and establishes consistent states accordingly. \\n"
            + "This means that multiple files can be processed before a consistent state is established. \\n\\n"
            + "At checkpoint, the operator saves the last submitted filename and its modification timestamp to the checkpoint. \\n"
            + "Upon application failures, the operator resubmits all files that are newer than the last submitted file at checkpoint. \\n\\n"
            
            + "# Exceptions \\n\\n" 
            
            + "The operator terminates in the following cases: \\n"
            + "* The operator cannot connect to HDFS. \\n"
            + "* The **strictMode** parameter is true but the directory is not found. \\n"
            + "* The path that is given by the directory name exists, but is an ordinary file and not a directory. \\n"
            + "* HDFS failed to give a list of files in the directory. \\n"
            + "* The pattern that is specified in the pattern parameter fails to compile. \\n\\n"
            
            + "+ Examples \\n\\n" 
            
            + "This example uses the `HDFS2DirectoryScan` operator to scan the HDFS directory On IBM Cloud.  The **hdfsUser** and **hdfsPassword** parameters are used to provide the username and password for authentication. \\n"
            + " \\n"
            + "    (stream<rstring filename> Files) as HDFS2DirectoryScan_1 = HDFS2DirectoryScan() \\n"
            + "    { \\n" 
            + "        param \\n"
            + "            directory     : \\\"/user/myuser/\\\"; \\n"
            + "            hdfsUri       : \\\"webhdfs://hdfsServer:8443\\\"; \\n"
            + "            hdfsPassword  : \\\"password\\\"; \\n"
            + "            hdfsUser      : \\\"biuser\\\"; \\n"
            + "            sleepTime     : 2.0; \\n" + "    } \\n\\n" +

            "This example uses the `HDFS2DirectoryScan` operator to scan a HDFS directory every two seconds. \\n"
            + "The **hdfsUri** parameter in this case overrides the value that is specified by the `fs.defaultFS` option in the `core-site.xml`. \\n\\n\\n"
            +

            "    (stream<rstring filename> Files) as HDFS2DirectoryScan_1 = HDFS2DirectoryScan() \\n"
            + "    { \\n" 
            + "        param \\n"
            + "            directory     : \\\"/user/myuser/\\\"; \\n"
            + "            hdfsUri       : \\\"hdfs://hdfsServer:1883\\\"; \\n"
            + "            sleepTime     : 2.0; \\n" + "    } \\n";

    public static final String DESC_HDFS_DIR_SCAN_INPUT = "The `HDFS2DirectoryScan` operator has an optional control input port. You can use this port to change the directory "
            + "that the operator scans at run time without restarting or recompiling the application. \\n"
            + "The expected schema for the input port is of tuple<rstring directory>, a schema containing a single attribute of type rstring. \\n"
            + "If a directory scan is in progress when a tuple is received, the scan completes and a new scan starts immediately after "
            + "and uses the new directory that was specified. \\n"
            + "If the operator is sleeping, the operator starts scanning the new directory immediately after it receives an input tuple. \\n";

    public static final String DESC_HDFS_DIR_SCAN_OUTPUT = "    The `HDFS2DirectoryScan` operator has one output port. \\n"
            + "This port provides tuples of type rstring that are encoded in UTF-8 and represent the file names that are found in the directory, "
            + "one file name per tuple.  The file names do not occur in any particular order. \\n"
            + "The port is non-mutating and punctuation free. \\n";

    public static final String DESC_HDFS_DIR_SCAN_DIRECTORY = "This optional parameter specifies the name of the directory to be scanned. \\n"
            + "If the name starts with a slash, it is considered an absolute directory that you want to scan. If it does not start with a slash, it is considered a relative directory, relative to the '/user/*userid*/ directory. This parameter is mandatory if the input port is not specified. \\n";

    public static final String DESC_HDFS_DIR_SCAN_SLEEP_TIME = "This optional parameter specifies the minimum time between directory scans. The default value is 5.0 seconds. \\n";

    public static final String DESC_HDFS_DIR_SCAN_PATTERN = "This optional parameter limits the file names that are listed to the names that match the specified regular expression. \\n"
            + "The `HDFS2DirectoryScan` operator ignores file names that do not match the specified regular expression. \\n";

    public static final String DESC_HDFS_DIR_SCAN_STRICT_MODE = "This optional parameter determines whether the operator reports an error if the directory to be scanned does not exist. \\n"
            + "If you set this parameter to true and the specified directory does not exist or there is a problem accessing the directory, the operator reports an error and terminates. \\n"
            + "If you set this parameter to false and the specified directory does not exist or there is a problem accessing the directory, the operator treats it as an empty directory and does not report an error \\n";

    public static final String DESC_HDFS_FILE_COPY = "The **HDFS2FileCopy** operator copies files from a HDFS fiel system to the loca disk and also from a local disk to the HDFS file system \\n\\n"
            + "The  `HDFS2FileCopy`  uses the hadoop JAVA API functions `copyFromLocalFile` and `copyToLocalFile` to copy files in two directions. \\n"
            + "* `copyFromLocalFile` : Copies a file from local disk to the HDFS file system. \\n"
            + "* `copyToLocalFile`   : Copies a file from HDFS file system to the local disk. \\n\\n"

            + "+ Examples \\n\\n"
            + "This example copies the file test.txt from local path ./data/ into /user/hdfs/ directory. \\n\\n"
            + "    streams<boolean succeed> copyFromLocal =  HDFS2FileCopy()\\n"
            + "    {\\n\\n" + "        param\\n"
            + "            localFile                : \\\"test.txt\\\"; \\n"
            + "            hdfsFile                 : \\\"/user/hdfs/test.txt\\\"; \\n"
            + "            deleteSourceFile         : false; \\n"
            + "            overwriteDestinationFile : true; \\n"
            + "            direction                : copyFromLocalFile\\n"
            + "    }\\n\\n"
            + "This example copies all files from local path /tmp/work into /user/hdfs/work directory. \\n"

            + "    // DirectoryScan operator with an absolute file argument and a file name pattern \\n"
            + "    stream<rstring localFile> DirScan = DirectoryScan() \\n"
            + "    {  \\n"
            + "        param \\n"
            + "            directory       : \\\"/tmp/work\\\"; \\n"
            + "            initDelay       : 1.0; \\n"
            + "    } \\n\\n"

            + "    // copies all incoming files from input port into /user/hdfs/work directory. \\n"
            + "    stream<rstring message, uint64 elapsedTime> lineSink1 = HDFS2FileCopy(DirScan)\\n"
            + "    { \\n"
            + "        param\\n"
            + "              hdfsUser                 : \\\"hdfs\\\"; \\n"
            + "              hdfsFile                 : \\\"/user/hdfs/work/\\\"; \\n"
            + "              deleteSourceFile         : false; \\n"
            + "              overwriteDestinationFile : true; \\n"
            + "              direction                : copyFromLocalFile; \\n"
            + "              localFileAttrName        : \\\"localFile\\\"; \\n"

            + "    }\\n\\n"
            
            + "This example copies all files from HDFS directory `/user/hdfs/work` into local directory `/tmp/work2` . \\n\\n"
            + "* 1- copy the kerberos keytab file of your hdfs user from  HDFS server into etc directory. \\n"
            + "* 2- Replace the kerberos principal with your hdsf principal. \\n"
            + "* 3- Copy the `core-site.xml` file from your HDFS server into `etc` directory. \\n"
            + "* 4- Copy the kerberos configuration file in `/etc` directory of your streams server. \\n\\n\\n"

            + "`HDFS2DirectoryScan` operator with an absolute directory argument and the kerberos authentication parameters. \\n"
            + "    stream<rstring hdfsFile> HdfsDirScan = HDFS2DirectoryScan() \\n"
            + "    {  \\n"
            + "        param \\n"
            + "            configPath               : \\\"etc1\\\"; \\n"
            + "            authKeytab               : \\\"etc/hdfs.headless.keytab\\\"; \\n"
            + "            authPrincipal            : \\\"hdfs-hdpcluster@HDP2.COM\\\"; \\n"
            + "            vmArg                    : \\\"-Djava.security.krb5.conf=/etc/krb5.conf\\\"; \\n"
            + "            directory                : \\\"/user/hdfs/work\\\"; \\n"
            + "            sleepTime                : 2.0; \\n" 
            + "    } \\n\\n"

            + " // copies all incoming HDFS files from input port into /tmp/work2 directory. \\n"
            + "    stream<rstring message, uint64 elapsedTime> CopyToLocal = HDFS2FileCopy(HdfsDirScan) \\n"
            + "    { \\n"
            + "        param\\n"
            + "            authKeytab               : \\\"etc/hdfs.headless.keytab\\\"; \\n"
            + "            authPrincipal            : \\\"hdfs-hdpcluster@HDP2.COM\\\"; \\n"
            + "            configPath               : \\\"etc1\\\"; \\n"
            + "            vmArg                    : \\\"-Djava.security.krb5.conf=/etc/krb5.conf\\\"; \\n"
            + "            hdfsFileAttrName         : \\\"hdfsFile\\\"; \\n"
            + "            deleteSourceFile         : false; \\n"
            + "            overwriteDestinationFile : true; \\n"
            + "            direction                : copyToLocalFile; \\n"
            + "            localFile                : \\\"/tmp/work2\\\"; \\n"
            + "    }\\n";

    public static final String DESC_HDFS_FILE_COPY_INPUT = "The `HDFS2FileCopy` operator has one input port, which contents the file names that you specified. \\n"
            + "The input port is non-mutating, and its punctuation mode is `Oblivious` >. \\n\\n"
            + "The schema of the input port is:\\n\\n"
            + "`tuple<rstring localFileAttrName, rstring hdfsFileAttrName>`\\n\\n"
            + "or one of them:\\n" + "`tuple<rstring localFileAttrName>` or "
            + "`tuple<rstring hdfsFileAttrName>` "
            + ", which specifies a rstring for file names. \\n";

    public static final String DESC_HDFS_FILE_COPY_OUTPUT = "The `HDFS2FileCopy` operator is configurable with an optional output port. \\n"
            + "The output port is non-mutating and its punctuation mode is `Free`. \\n\\n"
            + "The schema of the output port is:\\n\\n `<string result, uint64 elapsedTime>` \\n\\n, which delivers the result of copy process and the elapsed time in milisecunds. \\n\\n"
            + "In case of any error it returns the error message as result";

    public static final String DESC_HDFS_COPYY_LOCAL_FILE = "This optional parameter specifies the name of local file to be copied. \\n"
            + "If the name starts with a slash, it is considered an absolute path of local file that you want to copy. \\n"
            + "If it does not start with a slash, it is considered a relative path, relative to your project data directory. \\n"
            + "If you want to copy all incoming files from input port to a directory set the value of direction to `copyToLocalFile` and  set the value of this parameter to a directory with a slash at the end e.g.  'data/testDir/. \\n"
            + "This parameter is mandatory if the 'localFileAttrNmae' is not specified in input port. \\n";

    public static final String DESC_HDFS_COPYY_HDFS_FILE = "This optional parameter specifies the name of HDFS file or directory. \\n"
            + "If the name starts with a slash, it is considered an absolute path of HDFS file that you want to use. \\n"
            + "If it does not start with a slash, it is considered a relative path, relative to the '/user/*userid*/hdfsFile. \\n"
            + "If you want to copy all incoming files from input port to a directory set the value of direction to `copyFromLocalFile` and set the value of this parameter to a directory with a slash at the end e.g.  '/user/userid/testDir/. \\n"
            + "This parameter is mandatory if the 'hdfsFileAttrNmae' is not specified in input port. \\n";

    public static final String DESC_HDFS_COPYY_LOCAL_ATTR_FILE = "This optional parameter specifies the value of localFile that coming through input stream. \\n"
            + "If the name starts with a slash, it is considered an absolute path of local file that you want to copy. \\n"
            + "If it does not start with a slash, it is considered a relative path, relative to your project data directory. \\n"
            + "This parameter is mandatory if the 'localFile' is not specified. \\n";

    public static final String DESC_HDFS_COPYY_HDFS_ATTR_FILE = "This optional parameter specifies the value of hdfsFile that coming through input stream. \\n"
            + "If the name starts with a slash, it is considered an absolute path of HDFS file that you want to copy. \\n"
            + "If it does not start with a slash, it is considered a relative path, relative to the '/user/*userid*/hdfsFile. \\n"
            + "This parameter is mandatory if the 'hdfsFile' is not specified. \\n";

    public static final String DESC_HDFS_COPYY_DIRECTION = "This parameter specifies the direction of copy. The parameter can be set with the following values. \\n"
            + "* `copyFromLocalFile`  Copy a file from local disk to the HDFS file system. \\n"
            + "* `copyToLocalFile` Copy a file from HDFS file system to the local disk. \\n";

    public static final String DESC_HDFS_COPYY_DELETE_SOURCE_FILE = "This optional parameter specifies whether to delete the source file when processing is finished.";

    public static final String DESC_HDFS_COPYY_OVERWRITE_DEST_FILE = "This optional parameter specifies whether to overwrite the destination file.";
}

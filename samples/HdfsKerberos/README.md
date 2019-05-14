## HsdfKerberso SPL sample 

This SPL sample demonstrates how to connect to the HDFS via kerberos authentication.

At fierst read this guide in WiKi to setup kerberos in your HDP server:

### How to use kerberos authentication in streamsx.hdfs toolkit
 
https://github.com/IBMStreams/streamsx.hdfs/wiki/How-to-use-kerberos-authentication-in-streamsx.hdfs-toolkit/

Please perform exactly the following steps.

   
   - 1- Add the host name and the IP Address of your HDP server in your /etc/hosts file.
   
     login as root in your streams server and.

         vi /etc/hosts
         xxx.xxx.xxx.xxx <your-hdp-server>
     
     Save the file and check it with ping
     
          ping <your-hdp-server>

   - 2- login as streams user and add the following lines at the end of your .bashrc file.
   
         vi ~/.bashrc
         export JAVA_HOME=$STREAMS_INSTALL/java
         export PATH=$JAVA_HOME/bin:$PATH

      Save the file and source it.

          source ~/.bashrc

      Now check the java location.

         which java


       The out put is like this line.

         /apps/software/InfoSphere_Streams/4.3.0.0/java/bin/java


   - 3- Copy core-site.xml file from Hadoop server on your Streams server in teh "etc" directory of your SPL application.
     For example 
     
         scp root@<your-hdp-server>:/etc/hadoop/conf/core-site.xml etc

   - 4- Copy the keytab file from Hadoop server on your Streams server in teh "etc" directory of your SPL application.

         scp root@<your-hdp-server>:/etc/security/ketabs/hdfs.headless.keytab etc

   - 5- Copy the kerberos configuration file krb5.conf file from Hadoop server on your Streams server in teh "etc" directory of your SPL application.

         scp root@<your-hdp-server>:/etc/krb5.conf etc


   - 6- Test the keytab
    For example:

           kinit -k -t hdfs.headless.keytab <your-hdfs-principal>
    
   If your configuration is correct, it creates a file in /tmp directory like this:

          /tmp/crb5_xxxx
          
  xxxx is your user id for example 1005  


   - 7- If you have any problem to access to the realm, copy the crb5.conf file from your HDFS server into a directory and add the following **vmArg** parameter to all your HDFS2 operators in your SPL files:
In this case the path of krb5.conf file is an absolute path 
   For example: 

         vmArg : "-Djava.security.krb5.conf=/etc/krb5.conf";

   **vmArgs** is a set of arguments to be passed to the Java VM within which this operator will be run.

   - 8-  replace the default vaule of $authKeytab and $authPrincipal in application/HdfsKerberps.spl file.
   
   - 9-  Make sure that the streams user have read/weite access to the HDFS file system. /user/<your-username>
 
     For eaxmple:
     
     login as root in your HDFS server.
    
     
         su - hdfs
         kinit -k -t /etc/security/ketabs/hdfs.headless.keytab <your-hdfs-principal>
         hadoop fs -ls /user/streamsadmin  

   - 10- Make the application and start it.

         make

     And start the appliaction in standalon mode

         output/bin/standalone

  It is also possible to start it in distributed mode.
  
        streamtool submitjob ./output/application.HdfsKerberos.sab
        
  Or you can submit a streams job with Submission Time parameters:
  For example:

       submitjob ./output/application.HdfsKerberos.sab -P configPath="etc" -P authKeytab="etc/hdfs.headless.keytab"


It reads the lines from the file data/LineInput.txt via **FileSource** operator.

Then the **HDFS2FileSink** operator writes all incoming lines into a file in your HDFS server in your user directory.

For example: 
    
    /user/streamsadmin/HDFS-LineInput.txt

In the next step the **HDFS2FileSource** reads lines from the HDFS file  /user/streamsadmin/HDFS-LineInput.txt.

In the last step the **FileSink** operator writes all incoming lines from HDFS file in your local system into data/result.txt


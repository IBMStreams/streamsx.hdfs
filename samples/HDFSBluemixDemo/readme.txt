These are the sample applications for the HDFS for Bluemix toolkit.
There are 3 composites in this project:
TestWrite
TestRead
TestDirScan.
Each composite tests one of the HDFS* operators: HDFS2FileSink, HDFS2FileSource, and HDFS2DirectoryScan respectively. 
It is suggested to run the TestWrite composite first inorder for the remaining applications to run successfully.

Each composite has been compiled and the resulting .sab files are in the 'deploy' directory.  
These applications are ready to be submitted as jobs in the Streaming Analytics service in Bluemix so you can quickly verify that they work with your BigInsights service.  See "Running in distributed mode" below.


Compiling in Streams Studio:
1. Start Streams Studio
2. In Streams Explorer view, add the "com.ibm.streamsx.hdfs" toolkit as one of the toolkit locations
3. From the main menu, select File -> Import.
4. Select General ->Existing Project into Workspace
5. Browse to the location of the HDFSBluemixDemo project and select it
6. Once the sample is imported, wait for the build to finish.  If autobuild is turned off, select resulting project, right click -> Build Project
7. Once the project is built, select the main composite of the sample, right click -> Launch Active Build Config 


Compiling at the command line:
1. Create a directory. For example, you can create a directory in your home directory. 
   mkdir $HOME/hdfssamples
2. Copy the samples to this directory. For example, you can copy the samples to the directory created above. 
   cp -R <path to hdfs bluemix toolkit>/samples/$HOME/hdfssamples/
3. Build the sample applications. Go to the HDFSBluemixDemo subdirectory and run the make. By default, the sample is compiled as a distributed application. If you want to compile the application as a standalone application, run make standalone instead. Run make clean to return the samples back to their original state.
4. Run the sample application. 



Running in distributed mode:
If using the Streaming Analytics service:
-Go to the Application Dashboard in your browser and click "submit job".  
-Browse to the deploy folder if using the pre compiled files or to the output/Distributed folder and upload the .sab file to the service.
-Specify the uri, username and password as submission parameters.

If using a local installation of Streams, start your IBM InfoSphere Streams instance, then use the streamtool command to submit the .adl files that were generated during the application build. 
    streamtool submitjob -i <instance_name> output/Distributed/hdfsexample::TestDirScan/hdfsexample.TestDirScan.adl -P hdfsUri="webhdfs://<machine_name>:<port>" -P hdfsUser="<user_name>" -P hdfsPassword="<password>"

Running in standalone mode: 
    ./output/Standalone/hdfsexample::<TestToRun>/bin/standalone hdfsUri="webhdfs://<machine_name>:<port>" hdfsUser="<user_name>" hdfsPassword="<password>"
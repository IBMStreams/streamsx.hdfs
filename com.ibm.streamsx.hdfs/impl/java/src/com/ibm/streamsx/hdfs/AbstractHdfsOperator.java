/*******************************************************************************
* Copyright (C) 2017, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.hdfs;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.hdfs.client.HdfsJavaClient;
import com.ibm.streamsx.hdfs.client.IHdfsClient;

public abstract class AbstractHdfsOperator extends AbstractOperator {

	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.AbstractHdfsOperator";
	public static final String EMPTY_STR = "";

	private static final String SCHEME_HDFS = "hdfs";
	private static final String SCHEME_GPFS = "gpfs";
	private static final String SCHEME_WEBHDFS = "webhdfs";
	public static final String RECONNPOLICY_NORETRY = "NoRetry";
	public static final String RECONNPOLICY_BOUNDEDRETRY = "BoundedRetry";
	public static final String RECONNPOLICY_INFINITERETRY = "InfiniteRetry";
	public static final int RECONN_BOUND_DEFAULT = 5;
	public static final double RECONN_INTERVAL_DEFAULT = 10;

	
	
	/**
	 * Create a logger specific to this class
	 */
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME); 

	private static Logger TRACE = Logger.getLogger(CLASS_NAME);

	// Common parameters and variables for connection
	private IHdfsClient fHdfsClient;
	private String fHdfsUri;
	private String fHdfsUser;
	private String fHdfsPassword;
	private String fAuthPrincipal;
	private String fAuthKeytab;
	private String fCredFile;
	private String fConfigPath;

	private String fReconnectionPolicy = RECONNPOLICY_BOUNDEDRETRY;
	// This optional parameter reconnectionBound specifies the number of successive connection
	// that will be attempted for this operator.
	// It can appear only when the reconnectionPolicy parameter is set to BoundedRetry
	// and cannot appear otherwise.
	// If not present the default value is 5
	private int fReconnectionBound = RECONN_BOUND_DEFAULT;
	// This optional parameter reconnectionInterval specifies the time period in seconds which
	// the operator will be wait before trying to reconnect.
	// If not specified, the default value is 10.0.
	private double fReconnectionInterval = RECONN_INTERVAL_DEFAULT;

	
	
	// Other variables
	protected Thread processThread = null;
	protected boolean shutdownRequested = false;
	private String fKeyStorePath;
	private String fKeyStorePassword;
	private String fLibPath; //Used to allow the user to override the hadoop home environment variable
	private String fPolicyFilePath;

	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		setupClassPaths(context);
		processPolicyFilePath();
		createConnection();
	}


	/** 
         * createConnection creates a connection to thr hadoop file system.
         *
        */
	private synchronized void createConnection() throws Exception {
		boolean connected = false;
		// Delay in miliseconds as specified in fReconnectionInterval parameter
		final long delay = TimeUnit.MILLISECONDS.convert((long) fReconnectionInterval, TimeUnit.SECONDS);
		System.out.println("createConnection  ReconnectionPolicy " +  fReconnectionPolicy 
				+ "  ReconnectionBound " +  fReconnectionBound 
				+ "  ReconnectionInterval " +  fReconnectionInterval);
		if (fReconnectionPolicy == RECONNPOLICY_NORETRY) {
			fReconnectionBound = 1;
		}

		if (fReconnectionPolicy == RECONNPOLICY_INFINITERETRY) {
			fReconnectionBound = 9999;
		}

		for(int nConnectionAttempts=0; nConnectionAttempts<fReconnectionBound; nConnectionAttempts++)
		{
			System.out.println("createConnection nConnectionAttempts is: " + nConnectionAttempts + " delay " + delay);

			try 
			{
				fHdfsClient = createHdfsClient();
				fHdfsClient.connect(getHdfsUri(), getHdfsUser(), getAbsolutePath(getConfigPath()));
				System.out.println("createConnection  connected to " +  fHdfsUri);
				LOGGER.log(TraceLevel.INFO, Messages.getString("HDFS_CLIENT_AUTH_CONNECT", fHdfsUri));

				connected = true;
				break;
			} catch (Exception e) 
			{
				System.out.println("Connection failed " + e.toString());
				connected = false;
				Thread.sleep(delay);
			}
		}

	}

	private void processPolicyFilePath() {
		String policyFilePath = getAbsolutePath(getPolicyFilePath());
		if (policyFilePath != null) {
			TRACE.log(TraceLevel.INFO, "Policy file path: " + policyFilePath);
			System.setProperty("com.ibm.security.jurisdictionPolicyDir", policyFilePath);
		}
	}

	private  void setupClassPaths(OperatorContext context) {
		String HADOOP_HOME = System.getenv("HADOOP_HOME");
		ArrayList<String> libList = new ArrayList<>();
		
		if (getLibPath() != null) {
			String user_defined_path = getLibPath()+ "/*";
			TRACE.log(TraceLevel.INFO, "Adding " + user_defined_path + " to classpath");
			libList.add(user_defined_path);
		} else if (HADOOP_HOME != null) {
			libList.add(HADOOP_HOME + "/../hadoop-conf");
			libList.add(HADOOP_HOME + "/etc/hadoop");
			libList.add(HADOOP_HOME + "/conf");
			libList.add(HADOOP_HOME + "/share/hadoop/hdfs/*");
			libList.add(HADOOP_HOME + "/share/hadoop/common/*");
			libList.add(HADOOP_HOME + "/share/hadoop/common/lib/*");
			libList.add(HADOOP_HOME + "/lib/*");
			libList.add(HADOOP_HOME + "/client/*");
			libList.add(HADOOP_HOME + "/*");
			libList.add(HADOOP_HOME + "/../hadoop-hdfs");
		} else {
			String default_dir = context.getToolkitDirectory() +"/impl/lib/ext/*";
			libList.add(default_dir);
		}

		try {
			context.addClassLibraries(libList.toArray(new String[0]));

		} catch (MalformedURLException e) {
			LOGGER.log(TraceLevel.ERROR, "LIB_LOAD_ERROR",  e);
		}
	}
	
	@Override
	public void allPortsReady() throws Exception {
		super.allPortsReady();
		if (processThread != null) {
			startProcessing();
		}
	}

	protected synchronized void startProcessing() {
		processThread.start();
	}

	/**
	 * By default, this does nothing.
	 */
	protected void process() throws Exception {

	}

	public void shutdown() throws Exception {

		shutdownRequested = true;
		if (fHdfsClient != null) {
			fHdfsClient.disconnect();
		}

		super.shutdown();
	}

	protected Thread createProcessThread() {
		Thread toReturn = getOperatorContext().getThreadFactory().newThread(
				new Runnable() {

					@Override
					public void run() {
						try {
							process();
						} catch (Exception e) {
							LOGGER.log(TraceLevel.ERROR, e.getMessage());
							// if we get to the point where we got an exception
							// here we should rethrow the exception to cause the
							// operator to shut down.
							throw new RuntimeException(e);
						}
					}
				});
		toReturn.setDaemon(false);
		return toReturn;
	}

	protected IHdfsClient createHdfsClient() throws Exception {
		IHdfsClient client = new HdfsJavaClient();

		client.setConnectionProperty(IHdfsConstants.KEYSTORE, getAbsolutePath(getKeyStorePath()));
		client.setConnectionProperty(IHdfsConstants.KEYSTORE_PASSWORD, getKeyStorePassword());

		client.setConnectionProperty(IHdfsConstants.HDFS_PASSWORD, getHdfsPassword());
		client.setConnectionProperty(IHdfsConstants.AUTH_PRINCIPAL, getAuthPrincipal());
		client.setConnectionProperty(IHdfsConstants.AUTH_KEYTAB, getAbsolutePath(getAuthKeytab()));
		client.setConnectionProperty(IHdfsConstants.CRED_FILE, getAbsolutePath(getCredFile()));

		return client;
	}

	protected String getAbsolutePath(String filePath) {
		if(filePath == null) 
			return null;

		Path p = new Path(filePath);
		if(p.isAbsolute()) {
			return filePath;
		} else {
			File f = new File (getOperatorContext().getPE().getApplicationDirectory(), filePath);
			return f.getAbsolutePath();
		}
	}

	protected IHdfsClient getHdfsClient() {
		return fHdfsClient;
	}

	@Parameter(optional = true)
	public void setHdfsUri(String hdfsUri) {
		TRACE.log(TraceLevel.DEBUG, "setHdfsUri: " + hdfsUri);
		fHdfsUri = hdfsUri;
	}

	public String getHdfsUri() {
		return fHdfsUri;
	}

	@Parameter(optional = true)
	public void setHdfsUser(String hdfsUser) {
		this.fHdfsUser = hdfsUser;
	}

	public String getHdfsUser() {
		return fHdfsUser;
	}

	//Parameter reconnectionPolicy
	@Parameter(optional = true, description="This optional parameter specifies the policy that is used by the operator to handle HDFS connection failures.  The valid values are: `NoRetry`, `InfiniteRetry`, and `BoundedRetry`. The default value is `BoundedRetry`. If `NoRetry` is specified and a HDFS connection failure occurs, the operator does not try to connect to the HDFS again.  The operator shuts down at startup time if the initial connection attempt fails. If `BoundedRetry` is specified and a HDFS connection failure occurs, the operator tries to connect to the HDFS again up to a maximum number of times. The maximum number of connection attempts is specified in the **reconnectionBound** parameter.  The sequence of connection attempts occurs at startup time. If a connection does not exist, the sequence of connection attempts also occurs before each operator is run.  If `InfiniteRetry` is specified, the operator continues to try and connect indefinitely until a connection is made.  This behavior blocks all other operator operations while a connection is not successful.  For example, if an incorrect connection password is specified in the connection configuration document, the operator remains in an infinite startup loop until a shutdown is requested.")
	public void setReconnectionPolicy(String reconnectionPolicy){
		this.fReconnectionPolicy = reconnectionPolicy;
	}

	public String getReconnectionPolicy() {
		return fReconnectionPolicy;
	}


	//Parameter reconnectionBound
	@Parameter(optional = true, description="This optional parameter specifies the number of successive connection attempts that occur when a connection fails or a disconnect occurs.  It is used only when the **reconnectionPolicy** parameter is set to `BoundedRetry`; otherwise, it is ignored. The default value is `5`.")
	public void setReconnectionBound(int reconnectionBound){
		this.fReconnectionBound = reconnectionBound;
	}

	public int getReconnectionBound() {
		return fReconnectionBound;
	}


	//Parameter reconnectionInterval
	@Parameter(optional = true, description="This optional parameter specifies the amount of time (in seconds) that the operator waits between successive connection attempts.  It is used only when the **reconnectionPolicy** parameter is set to `BoundedRetry` or `InfiniteRetry`; othewise, it is ignored.  The default value is `10`.")
	public void setReconnectionInterval(double reconnectionInterval){
		this.fReconnectionInterval = reconnectionInterval;
	}
	
	public double getReconnectionInterval() {
		return fReconnectionInterval;
	}
	
	
	@Parameter(optional = true)
	public void setAuthPrincipal(String authPrincipal) {
		this.fAuthPrincipal = authPrincipal;
	}

	public String getAuthPrincipal() {
		return fAuthPrincipal;
	}

	@Parameter(optional = true)
	public void setAuthKeytab(String authKeytab) {
		this.fAuthKeytab = authKeytab;
	}

	public String getAuthKeytab() {
		return fAuthKeytab;
	}

	@Parameter(optional = true)
	public void setCredFile(String credFile) {
		this.fCredFile = credFile;
	}

	public String getCredFile() {
		return fCredFile;
	}

	@Parameter(optional = true)
	public void setConfigPath(String configPath) {
		this.fConfigPath = configPath;
	}

	public String getConfigPath() {
		return fConfigPath;
	}
	
	public String getLibPath() {
		return fLibPath;
	}

	public String getHdfsPassword() {
		return fHdfsPassword;
	}

	public String getKeyStorePath() {
		return fKeyStorePath;
	}
	public String getKeyStorePassword() {
		return fKeyStorePassword;
	}

	public String getPolicyFilePath() {
		return fPolicyFilePath;
	}

	@Parameter(optional=true)	
	public void setHdfsPassword(String pass) {
		fHdfsPassword = pass;
	}

	@Parameter(optional=true)	
	public void setKeyStorePath(String path) {
		fKeyStorePath = path;
	}
	@Parameter(optional=true)	
	public void setKeyStorePassword(String pass) {
		fKeyStorePassword = pass;
	}
	
	@Parameter(optional=true) 
	public void setLibPath(String libPath) {
		fLibPath = libPath;
	}

	@Parameter(optional=true)
	public void setPolicyFilePath(String policyFilePath) {
		fPolicyFilePath = policyFilePath;
	}
}

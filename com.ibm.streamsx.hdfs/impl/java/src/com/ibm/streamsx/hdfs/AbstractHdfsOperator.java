/*******************************************************************************
 * Copyright (C) 2017-2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.StateHandler;
import com.ibm.streamsx.hdfs.client.HdfsJavaClient;
import com.ibm.streamsx.hdfs.client.IHdfsClient;

public abstract class AbstractHdfsOperator extends AbstractOperator implements StateHandler {

	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.AbstractHdfsOperator";
	public static final String EMPTY_STR = "";

	/** Create a logger specific to this class */
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME);

	private static Logger TRACE = Logger.getLogger(CLASS_NAME);

	// Common parameters and variables for connection
	private IHdfsClient fHdfsClient;
	public FileSystem fs;
	private String fHdfsUri;
	private String fHdfsUser;
	private String fHdfsPassword;
	private String fAuthPrincipal;
	private String fAuthKeytab;
	private String fCredFile;
	private String fConfigPath;

	private String fReconnectionPolicy = IHdfsConstants.RECONNPOLICY_BOUNDEDRETRY;
	// This optional parameter reconnectionBound specifies the number of
	// successive connection
	// that will be attempted for this operator.
	// It can appear only when the reconnectionPolicy parameter is set to
	// BoundedRetry
	// and cannot appear otherwise.
	// If not present the default value is 5
	private int fReconnectionBound = IHdfsConstants.RECONN_BOUND_DEFAULT;
	// This optional parameter reconnectionInterval specifies the time period in
	// seconds which
	// the operator will be wait before trying to reconnect.
	// If not specified, the default value is 10.0.
	private double fReconnectionInterval = IHdfsConstants.RECONN_INTERVAL_DEFAULT;
	// This parameter specifies the json string that contains the user, password
	// and hdfsUrl.
	private String credentials = null;;
	// The name of the application configuration object
	private String appConfigName = null;
	// data from application config object
	Map<String, String> appConfig = null;

	// Other variables
	protected Thread processThread = null;
	protected boolean shutdownRequested = false;
	private String fKeyStorePath;
	private String fKeyStorePassword;
	private String fLibPath; // Used to allow the user to override the hadoop
							 // home environment variable
	private String fPolicyFilePath;

	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		super.initialize(context);
		setupClassPaths(context);
		setJavaSystemProperty();
		loadAppConfig(context);
		if (credentials != null) {
			this.getCredentials(credentials);
		}
		createConnection();
	}

	/** createConnection creates a connection to the hadoop file system. */
	private synchronized void createConnection() throws Exception {
		// Delay in miliseconds as specified in fReconnectionInterval parameter
		final long delay = TimeUnit.MILLISECONDS.convert((long) fReconnectionInterval, TimeUnit.SECONDS);
		System.out.println("createConnection  ReconnectionPolicy " + fReconnectionPolicy + "  ReconnectionBound "
				+ fReconnectionBound + "  ReconnectionInterval " + fReconnectionInterval);
		if (fReconnectionPolicy == IHdfsConstants.RECONNPOLICY_NORETRY) {
			fReconnectionBound = 1;
		}

		if (fReconnectionPolicy == IHdfsConstants.RECONNPOLICY_INFINITERETRY) {
			fReconnectionBound = 9999;
		}

		for (int nConnectionAttempts = 0; nConnectionAttempts < fReconnectionBound; nConnectionAttempts++) {
			LOGGER.log(TraceLevel.INFO, "createConnection   nConnectionAttempts is: " + nConnectionAttempts + " delay "
					+ delay);
			try {
				fHdfsClient = createHdfsClient();
				fs = fHdfsClient.connect(getHdfsUri(), getHdfsUser(), getAbsolutePath(getConfigPath()));
				LOGGER.log(TraceLevel.INFO, Messages.getString("HDFS_CLIENT_AUTH_CONNECT", fHdfsUri));
				break;
			} catch (Exception e) {
				LOGGER.log(TraceLevel.ERROR, Messages.getString("HDFS_CLIENT_AUTH_CONNECT", e.toString()));
				Thread.sleep(delay);
			}
		}

	}
	
	

	/** set policy file path and https.protocols in JAVA system properties*/
	private void setJavaSystemProperty() {
		String policyFilePath = getAbsolutePath(getPolicyFilePath());
		if (policyFilePath != null) {
			TRACE.log(TraceLevel.INFO, "Policy file path: " + policyFilePath);
			System.setProperty("com.ibm.security.jurisdictionPolicyDir", policyFilePath);
		}
		System.setProperty("https.protocols","TLSv1.2");
		String httpsProtocol = System.getProperty("https.protocols");
		TRACE.log(TraceLevel.INFO, "streamsx.hdfs https.protocols " + httpsProtocol);	
	}

	private void setupClassPaths(OperatorContext context) {

		ArrayList<String> libList = new ArrayList<>();
		String HADOOP_HOME = System.getenv("HADOOP_HOME");
		if (getLibPath() != null) {
			String user_defined_path = getLibPath() + "/*";
			TRACE.log(TraceLevel.INFO, "Adding " + user_defined_path + " to classpath");
			libList.add(user_defined_path);
		} else {
			// add class path for delivered jar files from /impl/lib/ext/
			// directory
			String default_dir = context.getToolkitDirectory() + "/impl/lib/ext/*";
			TRACE.log(TraceLevel.INFO, "Adding /impl/lib/ext/* to classpath");
			libList.add(default_dir);

			if (HADOOP_HOME != null) {
				// if no config path and no HdfsUri is defined it checks the
				// HADOOP_HOME/config
				// directory for default core-site.xml file
				if ((getConfigPath() == null) && (getHdfsUri() == null)) {
					libList.add(HADOOP_HOME + "/conf");
					libList.add(HADOOP_HOME + "/../hadoop-conf");
					libList.add(HADOOP_HOME + "/etc/hadoop");
					libList.add(HADOOP_HOME + "/share/hadoop/hdfs/*");
					libList.add(HADOOP_HOME + "/share/hadoop/common/*");
					libList.add(HADOOP_HOME + "/share/hadoop/common/lib/*");
					libList.add(HADOOP_HOME + "/*");
					libList.add(HADOOP_HOME + "/../hadoop-hdfs");
				}
				libList.add(HADOOP_HOME + "/lib/*");
				libList.add(HADOOP_HOME + "/client/*");

			}
		}
		for (int i = 0; i < libList.size(); i++) {
			TRACE.log(TraceLevel.INFO, "calss path list " + i + " : " + libList.get(i));
		}

		try {
			context.addClassLibraries(libList.toArray(new String[0]));

		} catch (MalformedURLException e) {
			LOGGER.log(TraceLevel.ERROR, "LIB_LOAD_ERROR", e);
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

	/** By default, this does nothing. */
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
		Thread toReturn = getOperatorContext().getThreadFactory().newThread(new Runnable() {

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
		if (filePath == null)
			return null;

		Path p = new Path(filePath);
		if (p.isAbsolute()) {
			return filePath;
		} else {
			File f = new File(getOperatorContext().getPE().getApplicationDirectory(), filePath);
			return f.getAbsolutePath();
		}
	}

	protected IHdfsClient getHdfsClient() {
		return fHdfsClient;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_HDFS_URL)
	public void setHdfsUri(String hdfsUri) {
		TRACE.log(TraceLevel.DEBUG, "setHdfsUri: " + hdfsUri);
		fHdfsUri = hdfsUri;
	}

	public String getHdfsUri() {
		return fHdfsUri;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_HDFS_USER)
	public void setHdfsUser(String hdfsUser) {
		this.fHdfsUser = hdfsUser;
	}

	public String getHdfsUser() {
		return fHdfsUser;
	}

	// Parameter reconnectionPolicy
	@Parameter(optional = true, description = IHdfsConstants.DESC_REC_POLICY)
	public void setReconnectionPolicy(String reconnectionPolicy) {
		this.fReconnectionPolicy = reconnectionPolicy;
	}

	public String getReconnectionPolicy() {
		return fReconnectionPolicy;
	}

	// Parameter reconnectionBound
	@Parameter(optional = true, description = IHdfsConstants.DESC_REC_BOUND)
	public void setReconnectionBound(int reconnectionBound) {
		this.fReconnectionBound = reconnectionBound;
	}

	public int getReconnectionBound() {
		return fReconnectionBound;
	}

	// Parameter reconnectionInterval
	@Parameter(optional = true, description = IHdfsConstants.DESC_REC_INTERVAL)
	public void setReconnectionInterval(double reconnectionInterval) {
		this.fReconnectionInterval = reconnectionInterval;
	}

	public double getReconnectionInterval() {
		return fReconnectionInterval;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_PRINCIPAL)
	public void setAuthPrincipal(String authPrincipal) {
		this.fAuthPrincipal = authPrincipal;
	}

	public String getAuthPrincipal() {
		return fAuthPrincipal;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_AUTH_KEY)
	public void setAuthKeytab(String authKeytab) {
		this.fAuthKeytab = authKeytab;
	}

	public String getAuthKeytab() {
		return fAuthKeytab;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_CRED_FILE)
	public void setCredFile(String credFile) {
		this.fCredFile = credFile;
	}

	public String getCredFile() {
		return fCredFile;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_CONFIG_PATH)
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

	@Parameter(optional = true, description = IHdfsConstants.DESC_HDFS_PASSWORD)
	public void setHdfsPassword(String hadfsPassword) {
		fHdfsPassword = hadfsPassword;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_KEY_STOR_PATH)
	public void setKeyStorePath(String keyStorePath) {
		fKeyStorePath = keyStorePath;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_KEY_STOR_PASSWORD)
	public void setKeyStorePassword(String keyStorePassword) {
		fKeyStorePassword = keyStorePassword;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_LIB_PATH)
	public void setLibPath(String libPath) {
		fLibPath = libPath;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_POLICY_FILE_PATH)
	public void setPolicyFilePath(String policyFilePath) {
		fPolicyFilePath = policyFilePath;
	}

	// Parameter credentials
	@Parameter(name = "credentials", optional = true, description = IHdfsConstants.DESC_CREDENTIALS)
	public void setcredentials(String credentials) {
		this.credentials = credentials;
	}

	// Parameter appConfigName
	@Parameter(name = "appConfigName", optional = true, description = IHdfsConstants.DESC_APP_CONFIG_NAME)
	public void setAppConfigName(String appConfigName) {
		this.appConfigName = appConfigName;
	}

	/**
	 * read the credentials and set user name fHdfsUser, fHfsPassword and
	 * hdfsUrl.
	 * 
	 * @param credentials
	 */
	public void getCredentials(String credentials) throws IOException {
		String jsonString = credentials;
			try {
			JSONObject obj = JSONObject.parse(jsonString);
			fHdfsUser = (String) obj.get("user");
			if (fHdfsUser == null || fHdfsUser.trim().isEmpty()) {
				LOGGER.log(LogLevel.ERROR, Messages.getString("'fHdfsUser' is required to create HDFS connection."));
				throw new Exception(Messages.getString("'fHdfsUser' is required to create HDFS connection."));
			}

			fHdfsPassword = (String) obj.get("password");
			if (fHdfsPassword == null || fHdfsPassword.trim().isEmpty()) {
				LOGGER.log(LogLevel.ERROR, Messages.getString(
						"'fHdfsPassword' is required to create HDFS connection."));
				throw new Exception(Messages.getString("'fHdfsPassword' is required to create HDFS connection."));
			}

			fHdfsUri = (String) obj.get("webhdfs");
			if (fHdfsUri == null || fHdfsUri.trim().isEmpty()) {
				LOGGER.log(LogLevel.ERROR, Messages.getString("'fHdfsUri' is required to create HDFS connection."));
				throw new Exception(Messages.getString("'fHdfsUri' is required to create HDFS connection."));
			}
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * read the application config into a map
	 * 
	 * @param context the operator context
	 */
	protected void loadAppConfig(OperatorContext context) {
	
		// if no appconfig name is specified, create empty map
		if (appConfigName == null) {
			appConfig = new HashMap<String, String>();
			return;
		}

		appConfig = context.getPE().getApplicationConfiguration(appConfigName);
		if (appConfig.isEmpty()) {
			LOGGER.log(LogLevel.WARN, "Application config not found or empty: " + appConfigName);
		}

		for (Map.Entry<String, String> kv : appConfig.entrySet()) {
			TRACE.log(TraceLevel.DEBUG, "Found application config entry: " + kv.getKey() + "=" + kv.getValue());
		}

		if (null != appConfig.get("credentials")) {
			credentials = appConfig.get("credentials");
		}
	}

}

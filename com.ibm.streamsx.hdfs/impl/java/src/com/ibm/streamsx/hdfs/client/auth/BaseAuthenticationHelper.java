/*******************************************************************************
 * Copyright (C) 2014-2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs.client.auth;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.logging.Logger;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.hdfs.HDFSOperatorUtils;
import com.ibm.streamsx.hdfs.Messages;
import com.ibm.streamsx.hdfs.IHdfsConstants;

public abstract class BaseAuthenticationHelper implements IAuthenticationHelper {

	enum AuthType {
		SIMPLE, KERBEROS
	}

	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs..client.auth";

	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME);

	public static final String SIMPLE_AUTH = "simple";
	public static final String KERBEROS_AUTH = "kerberos";

	protected Configuration fConfiguration;
	protected URI fHdfsUri;

	public BaseAuthenticationHelper() {
		fConfiguration = new Configuration();

	}

	public BaseAuthenticationHelper(String configPath) {
		fConfiguration = new Configuration();

		if (configPath != null && !configPath.isEmpty()) {
			try {
				URL url = new URL("file", null, configPath + "/core-site.xml");
				fConfiguration.addResource(url);
			} catch (MalformedURLException e) {
				LOGGER.log(TraceLevel.ERROR, e.getMessage(), e);
			}
		}
	}

	@Override
	public FileSystem connect(String fileSystemUri, String hdfsUser, Map<String, String> connectionProperties)
			throws Exception {
		if (fileSystemUri != null && !fileSystemUri.isEmpty()) {
			fConfiguration.set("fs.defaultFS", fileSystemUri);
		}

		// if fs.defaultFS is not specified, try the deprecated name
		String uri = fConfiguration.get("fs.defaultFS");
		if (uri == null || uri.toString().isEmpty()) {
			uri = fConfiguration.get("fs.default.name");
		}

		if (uri == null) {
			throw new Exception("Unable to find a URI to connect to.");
		}

		setHdfsUri(new URI(uri));

		if (isConnectionToBluemix(hdfsUser, connectionProperties)) {
			fConfiguration.set("fs.webhdfs.impl", com.ibm.streamsx.hdfs.client.webhdfs.KnoxWebHdfsFileSystem.class
					.getName());
			fConfiguration.set(IHdfsConstants.KNOX_USER, hdfsUser);
			String keyStore = connectionProperties.get(IHdfsConstants.KEYSTORE);
			if (keyStore != null) {
				fConfiguration.set(IHdfsConstants.KEYSTORE, keyStore);
				String keyStorePass = connectionProperties.get(IHdfsConstants.KEYSTORE_PASSWORD);
				if (keyStorePass == null) {
					keyStorePass = "";
				}
				fConfiguration.set(IHdfsConstants.KEYSTORE_PASSWORD, keyStorePass);
			} else {
				// LOGGER.log(TraceLevel.WARN, "INSECURE_SSL_CONNECTION");
				LOGGER.log(TraceLevel.DEBUG, Messages.getString("INSECURE_SSL_CONNECTION"));
			}
			fConfiguration.set(IHdfsConstants.KNOX_PASSWORD, connectionProperties.get(IHdfsConstants.HDFS_PASSWORD));
		}
		LOGGER.log(TraceLevel.DEBUG, Messages.getString("HDFS_CLIENT_AUTH_ATTEMPTING_CONNECT", getHdfsUri()
				.toString()));

		return null;
	}

	protected boolean isConnectionToBluemix(String hdfsUser, Map<String, String> connectionProperties) {
		return hdfsUser != null && connectionProperties.get(IHdfsConstants.HDFS_PASSWORD) != null;
	}

	protected FileSystem internalGetFileSystem(URI hdfsUri, String hdfsUser) throws Exception {
		FileSystem fs = null;

		if (HDFSOperatorUtils.isValidHdfsUser(hdfsUser)) {
			LOGGER.log(TraceLevel.DEBUG, Messages.getString("HDFS_CLIENT_AUTH_CONNECT", hdfsUri + " " + hdfsUser));
			fs = FileSystem.get(hdfsUri, fConfiguration, hdfsUser);
		} else {
			LOGGER.log(TraceLevel.DEBUG, Messages.getString("HDFS_CLIENT_AUTH_CONNECT", hdfsUri));
			fs = FileSystem.get(hdfsUri, fConfiguration);
		}
		// check if root path /user exist
		try {
			if (!fs.exists(new Path(hdfsUri + "/user"))) {
				throw new FileNotFoundException("Path not found.");
			} else {
				FsStatus fsStatus = fs.getStatus();
				LOGGER.log(TraceLevel.INFO, "Number of remaining bytes on the file system: " + fsStatus.getRemaining());
				System.out.println("Filesystem  Home Directory :  " + fs.getHomeDirectory() );
				}

		} catch (FileNotFoundException e) {
			LOGGER.log(TraceLevel.ERROR, e.getMessage(), e);
		}

		return fs;
	}

	protected UserGroupInformation authenticateWithKerberos(final String hdfsUser, final String kerberosPrincipal,
			final String kerberosKeytab) throws Exception {
		UserGroupInformation.setConfiguration(fConfiguration);
		UserGroupInformation ugi;
		if (HDFSOperatorUtils.isValidHdfsUser(hdfsUser)) {
			UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);
			ugi = UserGroupInformation.createProxyUser(hdfsUser, UserGroupInformation.getLoginUser());
			LOGGER.log(TraceLevel.DEBUG, Messages.getString("HDFS_CLIENT_AUTH_PROXY_CONNECT", hdfsUser));
		} else {
			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosPrincipal, kerberosKeytab);
			LOGGER.log(TraceLevel.DEBUG, Messages.getString("HDFS_CLIENT_AUTH_USING_KERBOSER", kerberosPrincipal));
		}

		return ugi;
	}

	public URI getHdfsUri() {
		return fHdfsUri;
	}

	public void setHdfsUri(URI fHdfsUri) {
		this.fHdfsUri = fHdfsUri;
	}

	public Configuration getConfiguration() {
		return fConfiguration;
	}

	public AuthType getAuthType() {
		String auth = fConfiguration.get("hadoop.security.authentication");

		if (auth != null && auth.toLowerCase().equals("kerberos")) {
			return AuthType.KERBEROS;
		} else {
			return AuthType.SIMPLE;
		}
	}
}

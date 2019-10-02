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
import java.io.File;
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
				String coreSiteXmlFileName = configPath + "/core-site.xml";
				String hdfsSiteXmlFileName = configPath + "/hdfs-site.xml";
			
				File coreSiteXmlFile = new File(coreSiteXmlFileName);
				if (coreSiteXmlFile.exists()){
					URL coreSiteXmlUrl = new URL("file", null, coreSiteXmlFileName);
					fConfiguration.addResource(coreSiteXmlUrl);
					LOGGER.log(TraceLevel.INFO, "core-site.xml file: " + coreSiteXmlFileName);
					System.out.println("core-site.xml file: " + coreSiteXmlFileName);
				}
				else{
					LOGGER.log(TraceLevel.ERROR, "core-site.xml file doesn't exist in " + configPath);					
				}

				File hdfsSiteXmlFile = new File(hdfsSiteXmlFileName);
				if (hdfsSiteXmlFile.exists()){
					URL hdsfSiteXmlUrl = new URL("file", null, hdfsSiteXmlFileName);
					fConfiguration.addResource(hdsfSiteXmlUrl);
					LOGGER.log(TraceLevel.INFO, "hdfs-site.xml file: " + hdfsSiteXmlFile);
					System.out.println("hdfs-site.xml file: " + hdfsSiteXmlFile);
				}
				
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
			String keyStore = connectionProperties.get(IHdfsConstants.PARAM_KEY_STOR_PATH);
			if (keyStore != null) {
				fConfiguration.set(IHdfsConstants.PARAM_KEY_STOR_PATH, keyStore);
				String keyStorePass = connectionProperties.get(IHdfsConstants.PARAM_KEY_STOR_PASSWORD);
				if (keyStorePass == null) {
					keyStorePass = "";
				}
				fConfiguration.set(IHdfsConstants.PARAM_KEY_STOR_PASSWORD, keyStorePass);
			} else {
				// LOGGER.log(TraceLevel.WARN, "INSECURE_SSL_CONNECTION");
				LOGGER.log(TraceLevel.DEBUG, Messages.getString("INSECURE_SSL_CONNECTION"));
			}
			fConfiguration.set(IHdfsConstants.KNOX_PASSWORD, connectionProperties.get(IHdfsConstants.PARAM_HDFS_PASSWORD));
		}
		LOGGER.log(TraceLevel.DEBUG, Messages.getString("HDFS_CLIENT_AUTH_ATTEMPTING_CONNECT", getHdfsUri()
				.toString()));

		return null;
	}

	protected boolean isConnectionToBluemix(String hdfsUser, Map<String, String> connectionProperties) {
		return hdfsUser != null && connectionProperties.get(IHdfsConstants.PARAM_HDFS_PASSWORD) != null;
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

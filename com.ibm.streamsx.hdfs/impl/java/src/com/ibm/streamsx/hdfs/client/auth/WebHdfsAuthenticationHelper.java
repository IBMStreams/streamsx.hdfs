/*******************************************************************************
 * Copyright (C) 2014-2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs.client.auth;

import java.io.FileReader;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import com.ibm.streamsx.hdfs.IHdfsConstants;

public class WebHdfsAuthenticationHelper extends BaseAuthenticationHelper {

	public WebHdfsAuthenticationHelper() {
		super();
	}

	WebHdfsAuthenticationHelper(String configPath) {
		super(configPath);
	}

	@Override
	public FileSystem connect(String fileSystemUri, String hdfsUser, Map<String, String> connectionProperties)
			throws Exception {
		super.connect(fileSystemUri, hdfsUser, connectionProperties);

		// in order to authenticate with WebHDFS, need to set the filesystem
		// to the local filesystem so the cred file can be found
/*		fConfiguration.set("fs.defaultFS", "file:///");

		if (connectionProperties != null) {
			String credFilePath = connectionProperties.get(IHdfsConstants.PARAM_CRED_FILE);
			setHdfsUri(formatUriWithCredFile(getHdfsUri(), credFilePath));

			// set to the hdfsUser to the user in the credfile,
			// otherwise relative paths will be setup incorrectly
			if (hdfsUser == null && credFilePath != null) {
				hdfsUser = getUsernameFromCredFile(credFilePath);
			}
		}
*/
		FileSystem fs = null;
		if (getAuthType() == AuthType.KERBEROS) {
			if (connectionProperties != null) {
				String kerberosPrincipal = connectionProperties.get(IHdfsConstants.PARAM_AUTH_PRINCIPAL);
				String kerberosKeytab = connectionProperties.get(IHdfsConstants.PARAM_AUTH_KEYTAB);

				if (kerberosPrincipal == null || kerberosKeytab == null) {
					throw new Exception(
							"Kerberos authentication requires 'authPrincipal' and 'authKeytab' to be non-null");
				}

				UserGroupInformation ugi = authenticateWithKerberos(hdfsUser, kerberosPrincipal, kerberosKeytab);
				fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {

					@Override
					public FileSystem run() throws Exception {
						/* We are setting the hdfsUser to null
						 * since we are already running as that user.
						 * Providing a user here will result in an
						 * authentication error. */
						return internalGetFileSystem(getHdfsUri(), null);
					}

				});
			}
		} else {
			fs = internalGetFileSystem(getHdfsUri(), hdfsUser);
		}

		return fs;
	}
/*
	private String getUsernameFromCredFile(String credFilePath) throws Exception {
		Properties props = new Properties();
		props.load(new FileReader(credFilePath));

		return props.getProperty("username");
	}

	private URI formatUriWithCredFile(URI fileSystemUri, String credFilePath) throws Exception {
		String userInfo = null;
		if (credFilePath != null) {
			userInfo = "credfile:" + encodeCredFilePath(credFilePath);
		}

		URI webHdfsUri = new URI(fileSystemUri.getScheme(), userInfo, fileSystemUri.getHost(), fileSystemUri.getPort(),
				"/", null, null);

		return webHdfsUri;
	}

	private String encodeCredFilePath(String credFilePath) {
		String encodedPath = credFilePath.replace("/", "%2F");

		return encodedPath;
	}
*/
	@Override
	public void disconnect() {
		// TODO Auto-generated method stub

	}

}

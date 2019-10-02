/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.hdfs.client.auth;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.hdfs.IHdfsConstants;

import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.logging.Logger;

public class HDFSAuthenticationHelper extends BaseAuthenticationHelper {

	private static Logger TRACE = Logger.getLogger("AuthenticationHelper.class");

	public HDFSAuthenticationHelper() {
		super();
	}

	public HDFSAuthenticationHelper(String configPath) {
		super(configPath);
	}

	@Override
	public FileSystem connect(String fileSystemUri, final String hdfsUser, Map<String, String> connectionProperties)
			throws Exception {
		super.connect(fileSystemUri, hdfsUser, connectionProperties);
		TRACE.log(TraceLevel.DEBUG, "Attempting to connect to URI: " + getHdfsUri().toString());

		FileSystem fs = null;
		if (getAuthType() == AuthType.KERBEROS) { // Connect using Kerberos
													 // authentication
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
		} else { // Connect using Simple authentication (i.e. no authentication)
			fs = internalGetFileSystem(getHdfsUri(), hdfsUser);
		}

		if (fs == null || fs instanceof LocalFileSystem)
			throw new IOException(
					"Unable to connect to HDFS.  Check that core-site.xml is present or provide the hdfsUri parameter");

		return fs;
	}

	/* (non-Javadoc)
	 * @see
	 * com.ibm.streamsx.hdfs.client.IAuthenticationHelper#disconnect() */
	public void disconnect() {
		fConfiguration = null;
	}

}

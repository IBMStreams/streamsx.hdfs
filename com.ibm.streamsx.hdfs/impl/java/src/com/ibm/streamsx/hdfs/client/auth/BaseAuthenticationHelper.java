/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2014, 2014                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
package com.ibm.streamsx.hdfs.client.auth;

import java.net.URI;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.hdfs.HDFSOperatorUtils;

public abstract class BaseAuthenticationHelper implements IAuthenticationHelper {
	/* begin_generated_IBM_copyright_code                               */
	public static final String IBM_COPYRIGHT =
		" Licensed Materials-Property of IBM                              " + //$NON-NLS-1$ 
		" 5724-Y95                                                        " + //$NON-NLS-1$ 
		" (C) Copyright IBM Corp.  2014, 2014    All Rights Reserved.     " + //$NON-NLS-1$ 
		" US Government Users Restricted Rights - Use, duplication or     " + //$NON-NLS-1$ 
		" disclosure restricted by GSA ADP Schedule Contract with         " + //$NON-NLS-1$ 
		" IBM Corp.                                                       " + //$NON-NLS-1$ 
		"                                                                 " ; //$NON-NLS-1$ 
	/* end_generated_IBM_copyright_code                                 */

	enum AuthType {
		SIMPLE, KERBEROS
	}

	private static Logger logger = Logger
			.getLogger("BaseAuthenticationHelper.class");
	
	public static final String SIMPLE_AUTH = "simple";
	public static final String KERBEROS_AUTH = "kerberos";

	protected Configuration fConfiguration;
	protected URI fHdfsUri;
	
	public BaseAuthenticationHelper() {
		fConfiguration = new Configuration();
	}
	
	public BaseAuthenticationHelper(String configPath) {
		fConfiguration = new Configuration();
		
		if(configPath != null && !configPath.isEmpty())
			fConfiguration.addResource(configPath + "/core-site.xml");
	}

	@Override
	public FileSystem connect(String fileSystemUri, String hdfsUser,
			Map<String, String> connectionProperties) throws Exception {
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
		logger.log(TraceLevel.DEBUG, "Attempting to connect to URI: " + getHdfsUri().toString());
		
		return null;
	}
	
	protected FileSystem internalGetFileSystem(URI hdfsUri, String hdfsUser) throws Exception {
		FileSystem fs = null;
		
		if (HDFSOperatorUtils.isValidHdfsUser(hdfsUser)) {
			logger.log(TraceLevel.DEBUG, "Connect to HDFS: " + hdfsUri
					+ " " + hdfsUser);
			fs = FileSystem.get(hdfsUri, fConfiguration, hdfsUser);
		} else {
			logger.log(TraceLevel.DEBUG, "Connect to HDFS: " + hdfsUri);
			fs = FileSystem.get(hdfsUri, fConfiguration);
		}
		
		return fs;
	}
	
	protected UserGroupInformation authenticateWithKerberos(final String hdfsUser,
			final String kerberosPrincipal, final String kerberosKeytab)
			throws Exception {
		UserGroupInformation ugi;
		if (HDFSOperatorUtils.isValidHdfsUser(hdfsUser)) {
			UserGroupInformation.loginUserFromKeytab(kerberosPrincipal,
					kerberosKeytab);
			ugi = UserGroupInformation.createProxyUser(hdfsUser,
					UserGroupInformation.getLoginUser());
			logger.log(TraceLevel.DEBUG, "Connecting as proxy user: " + hdfsUser);
		} else {
			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
					kerberosPrincipal, kerberosKeytab);
			logger.log(TraceLevel.DEBUG, "Connecting using kerberosPrincipal: "
					+ kerberosPrincipal);
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

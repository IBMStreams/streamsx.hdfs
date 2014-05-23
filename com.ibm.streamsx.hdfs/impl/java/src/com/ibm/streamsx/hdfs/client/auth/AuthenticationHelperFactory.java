/*******************************************************************************
 * Licensed Materials - Property of IBM
 * Copyright IBM Corp. 2014
 * US Government Users Restricted Rights - Use, duplication or
 * disclosure restricted by GSA ADP Schedule Contract with
 * IBM Corp.
 *******************************************************************************/

package com.ibm.streamsx.hdfs.client.auth;

import com.ibm.streamsx.hdfs.HDFSOperatorUtils;
import com.ibm.streamsx.hdfs.IHdfsConstants;

public class AuthenticationHelperFactory {

	public static IAuthenticationHelper createAuthenticationHelper(
			String hdfsUri, String hdfsUser, String configPath)
			throws Exception {
		IAuthenticationHelper authHelper = null;

		// if no URI is defined by the user, default to HDFS
		if (hdfsUri == null) {
			authHelper = new HDFSAuthenticationHelper(configPath);
		} else {
			String fsType = HDFSOperatorUtils.getFSType(hdfsUri);
			if (fsType.equals(IHdfsConstants.FS_WEBHDFS)) {
				authHelper = new WebHdfsAuthenticationHelper(configPath);
			} else {
				// HDFS and GPFS
				authHelper = new HDFSAuthenticationHelper(configPath);
			}
		}

		return authHelper;
	}
}

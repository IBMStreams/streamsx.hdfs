/*******************************************************************************
 * Copyright (C) 2014-2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs.client.auth;

import com.ibm.streamsx.hdfs.HDFSOperatorUtils;
import com.ibm.streamsx.hdfs.IHdfsConstants;

public class AuthenticationHelperFactory {

	public static IAuthenticationHelper createAuthenticationHelper(String hdfsUri, String hdfsUser, String configPath)
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

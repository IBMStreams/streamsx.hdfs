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

import com.ibm.streamsx.hdfs.HDFSOperatorUtils;
import com.ibm.streamsx.hdfs.IHdfsConstants;

public class AuthenticationHelperFactory {
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

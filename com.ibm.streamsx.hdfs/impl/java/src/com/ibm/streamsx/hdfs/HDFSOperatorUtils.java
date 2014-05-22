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
package com.ibm.streamsx.hdfs;

import java.net.URI;

public class HDFSOperatorUtils {
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

	public static boolean isValidHdfsUser(String hdfsUser) {
		if(hdfsUser == null || hdfsUser.trim().isEmpty()) 
			return false;

		return true;
	}	
	
	public static String getFSType(String uriStr) throws Exception {
		if(uriStr == null)
			throw new IllegalArgumentException("URI cannot be null");
		
		URI uri = new URI(uriStr);
		String scheme = uri.getScheme();
		
		if(scheme == null) {
			return IHdfsConstants.FS_HDFS;
		}
		
		if(scheme.toLowerCase().equals(IHdfsConstants.FS_WEBHDFS)) {
			return IHdfsConstants.FS_WEBHDFS;
		} else if(scheme.toLowerCase().equals(IHdfsConstants.FS_GPFS)) {
			return IHdfsConstants.FS_GPFS;
		} else {
			return IHdfsConstants.FS_HDFS;
		}
	}
	
}

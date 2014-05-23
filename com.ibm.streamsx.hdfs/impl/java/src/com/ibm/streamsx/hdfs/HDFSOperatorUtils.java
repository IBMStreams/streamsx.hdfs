/*******************************************************************************
 * Licensed Materials - Property of IBM
 * Copyright IBM Corp. 2014
 * US Government Users Restricted Rights - Use, duplication or
 * disclosure restricted by GSA ADP Schedule Contract with
 * IBM Corp.
 *******************************************************************************/

package com.ibm.streamsx.hdfs;

import java.net.URI;

public class HDFSOperatorUtils {

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

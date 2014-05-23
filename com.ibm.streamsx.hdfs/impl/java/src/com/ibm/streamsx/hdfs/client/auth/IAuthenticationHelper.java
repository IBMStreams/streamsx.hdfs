/*******************************************************************************
 * Licensed Materials - Property of IBM
 * Copyright IBM Corp. 2014
 * US Government Users Restricted Rights - Use, duplication or
 * disclosure restricted by GSA ADP Schedule Contract with
 * IBM Corp.
 *******************************************************************************/

package com.ibm.streamsx.hdfs.client.auth;

import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

public interface IAuthenticationHelper {

	public FileSystem connect(String fileSystemUri, final String hdfsUser,
			Map<String, String> connectionProperties) throws Exception;

	public void disconnect();

}

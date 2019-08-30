/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs.client.auth;

import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

public interface IAuthenticationHelper {

	public FileSystem connect(String fileSystemUri, final String hdfsUser, Map<String, String> connectionProperties)
			throws Exception;

	public void disconnect();

}

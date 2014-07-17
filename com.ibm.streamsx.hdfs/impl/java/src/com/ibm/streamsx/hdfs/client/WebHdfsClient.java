/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.hdfs.client;

import java.util.logging.Logger;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.hdfs.IHdfsConstants;
import com.ibm.streamsx.hdfs.client.auth.AuthenticationHelperFactory;
import com.ibm.streamsx.hdfs.client.auth.IAuthenticationHelper;
import com.ibm.streamsx.hdfs.client.auth.WebHdfsAuthenticationHelper;

public class WebHdfsClient extends AbstractHdfsClient {

	private Logger logger = Logger.getLogger("AbstractHdfsClient.class");

	public WebHdfsClient() {
	}
}

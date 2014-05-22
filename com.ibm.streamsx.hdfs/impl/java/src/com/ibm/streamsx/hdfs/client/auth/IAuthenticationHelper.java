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

import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

public interface IAuthenticationHelper {

	public FileSystem connect(String fileSystemUri, final String hdfsUser,
			Map<String, String> connectionProperties) throws Exception;

	public void disconnect();

}

/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2013, 2014                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
package com.ibm.streamsx.hdfs.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;

public interface IHdfsClient {
	
	public void connect(String fileSystemUri, String hdfsUser, String configPath) throws Exception;
	
	public InputStream getInputStream(String filePath) throws IOException;
	
	public OutputStream getOutputStream(String filePath, boolean append) throws IOException;	
	
	public FileStatus[] scanDirectory(String dirPath, String filter) throws IOException;
	
	public boolean exists(String filePath) throws IOException;
	
	public boolean isDirectory(String filePath) throws IOException;
	
	public long getFileSize(String filename) throws IOException;
	
	public void disconnect() throws Exception;
	
	public void setConnectionProperty(String name, String value);
	
	public String getConnectionProperty(String name);
	
	public Map<String, String> getConnectionProperties();
}

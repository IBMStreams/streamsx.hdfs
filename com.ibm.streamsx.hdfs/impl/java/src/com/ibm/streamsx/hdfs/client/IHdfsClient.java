/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.hdfs.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public interface IHdfsClient {
	
	public FileSystem connect(String fileSystemUri, String hdfsUser, String configPath) throws Exception;
	
	public InputStream getInputStream(String filePath) throws IOException;
	
	public OutputStream getOutputStream(String filePath, boolean append) throws IOException;	
	
	public FileStatus[] scanDirectory(String dirPath, String filter) throws IOException;
	
	public boolean exists(String filePath) throws IOException;
	
	public boolean isDirectory(String filePath) throws IOException;
	
	public long getFileSize(String filename) throws IOException;
	
	public boolean rename(String src, String dst) throws IOException;
	
	public boolean delete(String filePath, boolean recursive) throws IOException;
	
	public void disconnect() throws Exception;
	
	public void setConnectionProperty(String name, String value);
	
	public String getConnectionProperty(String name);
	
	public Map<String, String> getConnectionProperties();
}

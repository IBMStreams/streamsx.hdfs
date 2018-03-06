/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.hdfs.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.ibm.streamsx.hdfs.client.auth.AuthenticationHelperFactory;
import com.ibm.streamsx.hdfs.client.auth.IAuthenticationHelper;

abstract class AbstractHdfsClient implements IHdfsClient {
	

	protected FileSystem fFileSystem;
	protected boolean fIsDisconnected;
	protected IAuthenticationHelper fAuthHelper;
	
	private HashMap<String, String> fConnectionProperties = new HashMap<String, String>();

	@Override
	public void connect(String fileSystemUri, String hdfsUser, String configPath)
			throws Exception {
		fAuthHelper = AuthenticationHelperFactory.createAuthenticationHelper(fileSystemUri, hdfsUser, configPath);
		fFileSystem = fAuthHelper.connect(fileSystemUri, hdfsUser, getConnectionProperties());
		
	}
	
	@Override
	public InputStream getInputStream(String filePath) throws IOException {
		if (fIsDisconnected) {
			return null;
		}		
		return fFileSystem.open(new Path(filePath));
	}

	@Override
	public OutputStream getOutputStream(String filePath, boolean append)
			throws IOException {

		if (fIsDisconnected)
			return null;

		if (!append) {
			return fFileSystem.create(new Path(filePath));
		} else {
			// TODO: The client supports append, but the operator does not
			// cannot get it to work reliably
			Path path = new Path(filePath);
			// if file exist, create output stream to append to file
			if (fFileSystem.exists(path)) {
				return fFileSystem.append(path);
			} else {
				OutputStream stream = fFileSystem.create(path);
				return stream;
			}
		}
	}

	@Override
	public FileStatus[] scanDirectory(String dirPath, String filter)
			throws IOException {

		FileStatus[] files = new FileStatus[0];

		if (fIsDisconnected)
			return files;

		Path path = new Path(dirPath);

		if (fFileSystem.exists(path)) {

			if (filter == null || filter.isEmpty()) {
				files = fFileSystem.listStatus(new Path(dirPath));
			} else {
				PathFilter pathFilter = new RegexExcludePathFilter(filter);
				files = fFileSystem.listStatus(new Path(dirPath), pathFilter);
			}
		}
		return files;
	}

	@Override
	public boolean exists(String filePath) throws IOException {

		if (fIsDisconnected)
			return true;

		return fFileSystem.exists(new Path(filePath));
	}

	@Override
	public long getFileSize(String filename) throws IOException {

		if (fIsDisconnected)
			return 0;

		FileStatus fileStatus = fFileSystem.getFileStatus(new Path(filename));
		return fileStatus.getLen();
	}

	@Override
	public boolean rename(String src, String dst) throws IOException {

		if (fIsDisconnected)
			return false;

		Path srcPath = new Path(src);
		Path dstPath = new Path(dst);
		Path parentPath = dstPath.getParent();
		if (parentPath != null) {
			if ( ! fFileSystem.mkdirs(parentPath)) {
				return false;
			}
		}
		return fFileSystem.rename(srcPath, dstPath);
	}

	@Override
	public boolean delete(String filePath, boolean recursive) throws IOException {
		if (fIsDisconnected)
			return false;
		
		Path f = new Path(filePath);
		return fFileSystem.delete(f, recursive);
	}

	@Override
	public boolean isDirectory(String filePath) throws IOException {

		if (fIsDisconnected)
			return false;

		return fFileSystem.getFileStatus(new Path(filePath)).isDir();
	}
	
	@Override
	public void disconnect() throws Exception {
		fFileSystem.close();
		fIsDisconnected = true;
		if(fAuthHelper != null)
			fAuthHelper.disconnect();
	}
	
	@Override
	public void setConnectionProperty(String name, String value) {
		fConnectionProperties.put(name, value);
	}
	
	@Override
	public String getConnectionProperty(String name) {
		return fConnectionProperties.get(name);
	}
	
	@Override
	public Map<String, String> getConnectionProperties() {
		return new HashMap<String, String>(fConnectionProperties);
	}
}

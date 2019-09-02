/*******************************************************************************
* Copyright (C) 2014-2019, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/
package com.ibm.streamsx.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.hdfs.client.IHdfsClient;


enum EnumFileExpirationPolicy {
	NEVER, SIZE, TUPLECNT, PUNC, TIME
}

public class HdfsFile {

	private IHdfsClient fHdfsClient;
	private String fPath;
	private AsyncBufferWriter fWriter;

	private boolean fIsExpired;
	private EnumFileExpirationPolicy expPolicy = EnumFileExpirationPolicy.NEVER;
	
	private static final String UTF_8 = "UTF-8";

	// punctuation will simply set the file as expired
	// and not tracked by the file.
	private long size;
	private long sizePerFile;

	private long tupleCnt;
	private long tuplesPerFile;

	private double timePerFile;
	private String fEncoding = UTF_8;
	
	int numTuples = 0;
	private byte[] fNewLine;

	/// The metatype of the attribute we'll be working with.
	private final MetaType attrType;
	
	/// The index of the attribute that matters.
	private final int attrIndex;
	
	private OperatorContext fOpContext;
	
	private boolean fIsBinary;
	private boolean isAppend = false; 	// default is false, overwrite file
	

	/**
	 * Create an instance of HdfsFile
	 * @param context	Operator context.
	 * @param path		name of the file
	 * @param client	hdfs connection
	 * @param encoding	The file encoding; only matters for text files.
	 * @param attrIndex	The index of the attribute we'll be writing.
	 * @param attrType	The index of the attribute we'll be writing.
	 */
	public HdfsFile(OperatorContext context, String path, IHdfsClient client, String encoding,int attrIndex, MetaType attrType) {
		fPath = path;
		fHdfsClient = client;
		fOpContext = context;
		
		if (encoding == null)
		{
			encoding = UTF_8;
		}
		
		fEncoding = encoding;

		try {
			fNewLine = System.getProperty("line.separator").getBytes(fEncoding);
		} catch (UnsupportedEncodingException e) {
			fNewLine = System.getProperty("line.separator").getBytes();
		}
		this.attrIndex = attrIndex;
		this.attrType = attrType;
	}

	public void writeTuple(Tuple tuple) throws Exception {
		if (fWriter == null) {
			if (MetaType.BLOB == attrType) {
				initWriter(true, isAppend);
			}
			else {
				initWriter(false, isAppend);
			}
		}
		
		byte[] tupleBytes = null;
		
		switch (attrType) {
		case BLOB: 
			ByteBuffer buffer = tuple.getBlob(attrIndex).getByteBuffer();
			tupleBytes = new byte[buffer.limit()];
			buffer.get(tupleBytes);
			break;
		case RSTRING:
			Object attrObj = tuple.getObject(attrIndex);
			if (fEncoding.equals(UTF_8))
			{
				tupleBytes = ((RString)attrObj).getData();
			}
			else
			{
				tupleBytes = ((RString)attrObj).getData();
				tupleBytes = new String(tupleBytes, UTF_8).getBytes(fEncoding);
			}
			break;
		case USTRING:
			String attrString = tuple.getString(attrIndex);
			tupleBytes = attrString.getBytes(fEncoding);
			break;
		default:
			throw new Exception("Unsupported type "+attrType);
		}
		
		fWriter.write(tupleBytes);

		numTuples++;
		long tupleSize=tupleBytes.length;
		size += tupleSize+fNewLine.length;
		
		// check expiration after write, so the next write
		// will create a new file
		// check expiration after write, so the next write
		// will create a new file				
		if (expPolicy != EnumFileExpirationPolicy.NEVER)
		{
			switch (expPolicy) {
			case TUPLECNT:
				tupleCnt++;
				if (tupleCnt >= tuplesPerFile) {
					setExpired();
				}
				break;
			case SIZE:
				if((sizePerFile-size)<tupleSize)
				{
					setExpired();
				}
				break;			
			default:
				break;
			}
		}
	}


	public void setExpired() {
		fIsExpired = true;
	}

	public boolean isExpired() {
		return fIsExpired;
	}

	public IHdfsClient getHdfsClient() {
		return fHdfsClient;
	}
	
	/**
	 * This method returns the size of the file as tracked by the operator.
	 * When data is written to HDFS, if data is not flush and sync'ed with the flie system
	 * the hdfsclient.getFileSize method will not reflect the latest size of the file.
	 * The operator keeps track of how much data has been written to the file system,
	 * and use this information to determine when a file should be closed and a new file should
	 * be created
	 * @return the size of file as tracked by operator
	 */
	public long getSize() {
		
		return size;
	}
	
	/**
	 * This method goes to the HDFS client and returns the size of the file as known
	 * by the file system.  If any error occurs when fetching the size of the file,
	 * the method returns the last known size by the operator.  This method should NOT
	 * be used to determine the current size of the file, if the file is being written to
	 * and data is buffered.  This method is created for reporting purposes when the operator
	 * needs to return the actual size of the file from teh file system.
	 * @return size of file as known by file system.
	 */
	public long getSizeFromHdfs()
	{
		try {
			return fHdfsClient.getFileSize(fPath);
		} catch (IOException e) {
			
		}
		return getSize();
	}
	
	// can only be called by HDFS2FileSink on reset
	void setSize(long size) {
		this.size = size;
	}
	
	public long getTupleCnt() {
		return tupleCnt;
	}

	// can only be called by HDFS2FileSink on reset
	void setTupleCnt(long tupleCnt) {
		this.tupleCnt = tupleCnt;
	}

	/**
	 * Init the writer.
	 * Only one thread can create a new writer.  Write can be created by init, write or flush.  The synchronized keyword prevents
	 * write and flush to create writer at the same time. 
	 * @param isBinary  If true, file is considered a binary file.  If not, it is assumed to be a text file, and a newline is added after each write.
	 * @throws IOException
	 * @throws Exception
	 */
	synchronized private void initWriter(boolean isBinary, boolean append) throws IOException, Exception {
		
		if (fWriter == null)
		{
			OutputStream outStream = getHdfsClient().getOutputStream(fPath, append);
	
			if (outStream == null) {
				throw new Exception("Unable to open file for writing: " + fPath);
			} 
			// The AsyncBufferWriter writes a newline after every tuple.  For binary files, this is bad.
			// But, we just tell the AysncBufferWriter than the newline is an empty byte array, and we're good.
			if (isBinary) {
				fWriter = new AsyncBufferWriter(outStream, 1024*1024*16, fOpContext.getThreadFactory(), new byte[0]);
			}
			else {
				fWriter = new AsyncBufferWriter(outStream, 1024*1024*16, fOpContext.getThreadFactory(), fNewLine);
			}
		}
	}

	public void close() throws Exception {

		if (fWriter != null) {
			fWriter.close();
		}

		// do not close output stream, rely on the writer to close

	}

	public EnumFileExpirationPolicy getExpPolicy() {
		return expPolicy;
	}

	public void setExpPolicy(EnumFileExpirationPolicy expPolicy) {
		this.expPolicy = expPolicy;
	}

	public void setSizePerFile(long sizePerFile) {
		this.sizePerFile = sizePerFile;
	}

	/**
	 * Time before the file expire, timePerfile is expected to be in 
	 * miliseconds
	 * @param timePerFile  Time before the file expire, timePerfile is expected to be in 
	 */
	public void setTimePerFile(double timePerFile) {
		this.timePerFile = timePerFile;
	}

	public void setTuplesPerFile(long tuplesPerFile) {
		this.tuplesPerFile = tuplesPerFile;
	}
	
	public void setAppend(boolean append) {
		this.isAppend = append;
	}
	
	public boolean isAppend() {
		return isAppend;
	}
	
	public String getPath() {
		return fPath;
	}
	
	// called by drain method for consistent region
	public void flush() throws Exception {
		// close the current writer and recreate
		
		if (fWriter != null)
		{
			fWriter.flushAll();
		}
		
	}
	
	public boolean isClosed()
	{
		if (fWriter != null)
			return fWriter.isClosed();
		
		return true;
	}
}

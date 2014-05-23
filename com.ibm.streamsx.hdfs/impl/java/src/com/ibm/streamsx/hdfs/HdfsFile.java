/*******************************************************************************
 * Licensed Materials - Property of IBM
 * Copyright IBM Corp. 2014
 * US Government Users Restricted Rights - Use, duplication or
 * disclosure restricted by GSA ADP Schedule Contract with
 * IBM Corp.
 *******************************************************************************/
package com.ibm.streamsx.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
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

	
	private OperatorContext fOpContext;
	
	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.HdfsFile";
	
	public HdfsFile(OperatorContext context, String path, IHdfsClient client, String encoding) {
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
	}

	public void writeTuple(Tuple tuple) throws Exception {
		if (fWriter == null) {
			initWriter();
		}
				
		Object attrObj = tuple.getObject(0);
		byte[] tupleBytes = null;
		if (attrObj instanceof RString)
		{
			if (fEncoding.equals(UTF_8))
			{
				tupleBytes = ((RString)attrObj).getData();
			}
			else
			{
				tupleBytes = ((RString)attrObj).getData();
				tupleBytes = new String(tupleBytes, UTF_8).getBytes(fEncoding);
			}
		}
		else if (attrObj instanceof String)
		{			
			tupleBytes = ((String)attrObj).getBytes(fEncoding);
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
	public long getSize() {
		return size;
	}

	private void initWriter() throws IOException, Exception {
		OutputStream outStream = getHdfsClient().getOutputStream(fPath, false);

		if (outStream == null) {
			throw new Exception("Unable to open file for writing: " + fPath);
		} 
		
		fWriter = new AsyncBufferWriter(outStream, 1024*1024*16, fOpContext.getThreadFactory(), fNewLine);
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
}

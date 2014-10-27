/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
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
	
	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.HdfsFile";

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
				initWriter(true);
			}
			else {
				initWriter(false);
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
	public long getSize() {
		return size;
	}

	/**
	 * Init the writer. 
	 * @param isBinary  If true, file is considered a binary file.  If not, it is assumed to be a text file, and a newline is added after each write.
	 * @throws IOException
	 * @throws Exception
	 */
	private void initWriter(boolean isBinary) throws IOException, Exception {
		OutputStream outStream = getHdfsClient().getOutputStream(fPath, false);

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

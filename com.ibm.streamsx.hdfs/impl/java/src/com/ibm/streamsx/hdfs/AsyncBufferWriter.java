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
package com.ibm.streamsx.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;


public class AsyncBufferWriter extends Writer {
	/* begin_generated_IBM_copyright_code                               */
	public static final String IBM_COPYRIGHT =
		" Licensed Materials-Property of IBM                              " + //$NON-NLS-1$ 
		" 5724-Y95                                                        " + //$NON-NLS-1$ 
		" (C) Copyright IBM Corp.  2014, 2014    All Rights Reserved.     " + //$NON-NLS-1$ 
		" US Government Users Restricted Rights - Use, duplication or     " + //$NON-NLS-1$ 
		" disclosure restricted by GSA ADP Schedule Contract with         " + //$NON-NLS-1$ 
		" IBM Corp.                                                       " + //$NON-NLS-1$ 
		"                                                                 " ; //$NON-NLS-1$ 
	/* end_generated_IBM_copyright_code                                 */
	
	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.AsyncBufferWriter";
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME, "com.ibm.streamsx.hdfs.BigDataMessages");
	
	private static final int BUFFER_QUEUE_SIZE = 3;

	
	private byte[] buffer;
	private byte[] fNewline;
	private OutputStream out;
	private int size;
	private int position;
	private boolean isClosed = false;
	
	private ExecutorService exService;
	private LinkedBlockingQueue<byte[]> bufferQueue;
	
	private class FlushRunnable implements Runnable {
		
		protected byte[] flushBuffer;
		private boolean isAddBuffer;
		private int bufferPosition;
		
		public FlushRunnable(byte[] buffer, boolean addBuffer, int position) {
			flushBuffer = buffer;		
			isAddBuffer = addBuffer;
			bufferPosition = position;
		}

		@Override
		public void run() {
			try {
				out.write(flushBuffer, 0, bufferPosition);	
	
			} catch (IOException e) {
				LOGGER.log(LogLevel.ERROR, "Unable to write to HDFS output stream.", e);
			}		
			finally {
				if (isAddBuffer)
					addBuffer();
			}
		}

		private void addBuffer() {
			try {					
				if (!isClosed && bufferQueue.size() <= BUFFER_QUEUE_SIZE)
					bufferQueue.put(new byte[size]);
			} catch (InterruptedException e) {
				LOGGER.log(LogLevel.INFO, "Unable to add buffer to buffer queue", e);
			}
		}		
	}

	public AsyncBufferWriter(OutputStream outputStream, int size, ThreadFactory threadFactory, byte[] newline)  {
	
		out = outputStream;
		this.size = size;
		fNewline = newline;
		
		exService = Executors.newSingleThreadExecutor(threadFactory);
		bufferQueue = new LinkedBlockingQueue<byte[]>(BUFFER_QUEUE_SIZE);
		try {
			for (int i=0; i<BUFFER_QUEUE_SIZE; i++)
			{
				bufferQueue.put(new byte[size]);	
			}
			
			// take one buffer, two left in the queue
			buffer = bufferQueue.take();
		} catch (InterruptedException e) {
			LOGGER.log(LogLevel.ERROR, "Error setting up the buffer queue.", e);
		}
	}

	@Override
	public void close() throws IOException {		
		if (!isClosed)
		{
			isClosed = true;
			flush();			
			exService.shutdown();
			try {
				exService.awaitTermination(10000, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				LOGGER.log(LogLevel.WARN, "Execution Service shutdown is interrupted.", e);
			}finally {
				out.close();
				bufferQueue.clear();
			}
		}		
	}

	@Override
	public void flush() throws IOException {
		
		if (buffer.length > 0)
		{
			FlushRunnable runnable = new FlushRunnable(buffer, true, position);
			exService.execute(runnable);
			
			try {
				if (!isClosed)
					buffer = bufferQueue.take();
				position = 0;
			} catch (InterruptedException e) {
				LOGGER.log(LogLevel.ERROR, "Unable to retrieve buffer from buffer queue.", e);
			}
			
		}
	}

	@Override
	public void write(char[] src, int offset, int len) throws IOException {		
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Write the byte array to underlying output stream when buffer is full
	 * For each call to write method, a newline is appended
	 * @param src byte array to write
	 * @throws IOException 
	 */
	public void write(byte[] src) throws IOException {
		
		// if exceed buffer
		if((position+src.length+fNewline.length) > size)
		{
			// flush the buffer
			flush();
			
			// write new content
			FlushRunnable runnable = new FlushRunnable(src, false, src.length);
			exService.execute(runnable);
			
		}
		else {
			// store in buffer			
			System.arraycopy(src, 0, buffer, position, src.length);
			position += src.length;
			
			System.arraycopy(fNewline, 0, buffer, position, fNewline.length);
			position+= fNewline.length;
		}		
	}
}

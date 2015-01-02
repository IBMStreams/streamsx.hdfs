/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/
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

import org.apache.hadoop.fs.FSDataOutputStream;

import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;


public class AsyncBufferWriter extends Writer {
	
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
	private ThreadFactory fThreadFactory;
	
	private Object exServiceLock = new Object();
	
	private class FlushRunnable implements Runnable {
		
		protected byte[] flushBuffer;
		private boolean isAddBuffer;
		private int bufferPosition;
		private boolean newline;
		
		public FlushRunnable(byte[] buffer, boolean addBuffer, int position, boolean newline) {
			flushBuffer = buffer;		
			isAddBuffer = addBuffer;
			bufferPosition = position;
			this.newline = newline;
		}

		@Override
		public void run() {
			try {
				out.write(flushBuffer, 0, bufferPosition);	
				
				if (newline && fNewline.length > 0)
					out.write(fNewline, 0, fNewline.length);
				
				// force HDFS output stream to flush
				if (out instanceof FSDataOutputStream)
				{
					((FSDataOutputStream)out).hflush();
				}
				else {
					out.flush();
				}
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
		fThreadFactory = threadFactory;
		
		initExServiceAndBuffer(size, threadFactory);
	}

	private void initExServiceAndBuffer(int size, ThreadFactory threadFactory) {

		synchronized (exServiceLock) {
			exService = Executors.newSingleThreadExecutor(threadFactory);
			bufferQueue = new LinkedBlockingQueue<byte[]>(BUFFER_QUEUE_SIZE);
			try {
				for (int i = 0; i < BUFFER_QUEUE_SIZE; i++) {
					bufferQueue.put(new byte[size]);
				}

				// take one buffer, two left in the queue
				buffer = bufferQueue.take();
			} catch (InterruptedException e) {
				LOGGER.log(LogLevel.ERROR,
						"Error setting up the buffer queue.", e);
			}
		}
	}

	@Override
	public void close() throws IOException {		
		synchronized(exServiceLock) {
		if (!isClosed)
		{
			isClosed = true;
			
			// shut down the execution service, so no other flush runnable can be scheduled 
			// and wait for any flush job currently scheduled or running to finish
			exService.shutdown();
			try {
				exService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOGGER.log(LogLevel.WARN, "Execution Service shutdown is interrupted.", e);
			}finally {
				
				// do final flushing of buffer
				flushNow();
				out.close();
				bufferQueue.clear();
			}
		}		
	}
	}

	@Override
	public void flush() throws IOException {

		if (buffer.length > 0) {
			synchronized (exServiceLock) {
				FlushRunnable runnable = new FlushRunnable(buffer, true,
						position, false);
				exService.execute(runnable);

				try {
					if (!isClosed)
						buffer = bufferQueue.take();
					position = 0;
				} catch (InterruptedException e) {
					LOGGER.log(LogLevel.ERROR,
							"Unable to retrieve buffer from buffer queue.", e);
				}
			}
		}
	}
	
	protected void flushNow() throws IOException {
		if (buffer.length > 0)
		{
			FlushRunnable runnable = new FlushRunnable(buffer, false, position, false);
			runnable.run();
			position = 0;
		}
	}
	
	public void flushAll() throws IOException
	{
		synchronized(exServiceLock) {
			// shut down the execution service, so no other flush runnable can be scheduled 
			// and wait for any flush job currently scheduled or running to finish
			exService.shutdown();
			try {
				exService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOGGER.log(LogLevel.WARN, "Execution Service shutdown is interrupted.", e);
			}finally {

				// do final flushing of buffer
				flushNow();
				
				// after flushing, recreate exService
				initExServiceAndBuffer(size, fThreadFactory);
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
			synchronized (exServiceLock) {
				FlushRunnable runnable = new FlushRunnable(src, false,
						src.length, true);
				exService.execute(runnable);
			}
			
		}
		else {
			// store in buffer			
			System.arraycopy(src, 0, buffer, position, src.length);
			position += src.length;
			System.arraycopy(fNewline, 0, buffer, position, fNewline.length);
			position+= fNewline.length;

		}		
	}
	
	public boolean isClosed() {
		return isClosed;
	}
}

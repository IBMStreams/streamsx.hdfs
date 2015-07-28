/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;
import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streamsx.hdfs.client.IHdfsClient;

@SharedLoader
public class HDFS2FileSource extends AbstractHdfsOperator implements
		StateHandler {

	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.HDFSFileSource";
	private static final int BUFFER_SIZE = 1024*1024*8;

	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY
			+ "." + CLASS_NAME, "com.ibm.streamsx.hdfs.BigDataMessages");

	private static Logger TRACE = Logger.getLogger(HDFS2FileSource.class
			.getName());

	// TODO check that name matches filesource change required
	private static final String FILES_OPENED_METRIC = "nFilesOpened";
	private static final String BLOCKSIZE_PARAM = "blockSize";
	private Metric nFilesOpened;

	private String fFileName;
	private double fInitDelay;

	boolean fIsFirstTuple = true;
	boolean fBinaryFile = false;
	private int fBlockSize = 1024 * 4;

	private String fEncoding = "UTF-8";

	private ConsistentRegionContext fCrContext;

	private InputStream fDataStream;
	
	private long fSeekPosition = -1;
	private long fSeekToLine = -1;
	private long fLineNum = -1;
	private long fLineReaderPos = -1;
	private boolean fProcessThreadDone = false;
	private static String PARALLEL_READ_PARAM = "parallelRead";
	private boolean parallelRead = false;
	private BufferedLineReader bufferedLineReader = null;
	


	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		if (fFileName != null) {
			try {
				URI uri = new URI(fFileName);
				LOGGER.log(TraceLevel.DEBUG, "uri: " + uri.toString());

				String scheme = uri.getScheme();
				if (scheme != null) {
					String fs;
					if (uri.getAuthority() != null)
						fs = scheme + "://" + uri.getAuthority();
					else
						fs = scheme + ":///";

					if (getHdfsUri() == null)
						setHdfsUri(fs);

					LOGGER.log(TraceLevel.DEBUG, "fileSystemUri: "
							+ getHdfsUri());

					// Use original parameter value
					String path = fFileName.substring(fs.length());

					if (!path.startsWith("/"))
						path = "/" + path;

					setFile(path);
				}
			} catch (URISyntaxException e) {
				LOGGER.log(TraceLevel.DEBUG,
						"Unable to construct URI: " + e.getMessage());

				throw e;
			}
		}

		super.initialize(context);

		// Associate the aspect Log with messages from the SPL log
		// logger.
		setLoggerAspects(LOGGER.getName(), "HDFSFileSource");

		initMetrics(context);

		if (fFileName != null) {
			processThread = createProcessThread();
		}

		StreamSchema outputSchema = context.getStreamingOutputs().get(0)
				.getStreamSchema();
		MetaType outType = outputSchema.getAttribute(0).getType().getMetaType();
		// If we ever switch to the generated xml files, we'll be able to delete
		// this.
		if (context.getParameterNames().contains(BLOCKSIZE_PARAM)) {
			TRACE.fine("Blocksize parameter is supplied, setting blocksize based on that.");
			fBlockSize = Integer.parseInt(context.getParameterValues(
					BLOCKSIZE_PARAM).get(0));
		} else {
			TRACE.fine("Blocksize parameter not supplied, using default "
					+ fBlockSize);
		}
		if (context.getParameterNames().contains(PARALLEL_READ_PARAM)) {
			parallelRead = Boolean.parseBoolean(context.getParameterValues(PARALLEL_READ_PARAM).get(0));
		}
		if (MetaType.BLOB == outType) {
			fBinaryFile = true;
			TRACE.info("File will be read as a binary blobs of size "
					+ fBlockSize);
		} else {
			TRACE.info("Files will be read as text files, with one tuple per line.");
		}

		fCrContext = context.getOptionalContext(ConsistentRegionContext.class);
	}

	@ContextCheck(compile = true)
	public static void validateParameters(OperatorContextChecker checker)
			throws Exception {
		List<StreamingInput<Tuple>> streamingInputs = checker
				.getOperatorContext().getStreamingInputs();

		/*
		 * If there is no input port, then, the parameter file would become
		 * mandatory. Set context as invalid otherwise
		 */
		if (streamingInputs.size() == 0
				&& !checker.getOperatorContext().getParameterNames()
						.contains("file")) {
			checker.setInvalidContext(
					"HDFSFileSource requires either file parameter to be specified or the number of input ports to be one.",
					null);
		}

		/*
		 * If both input port and file parameter is specified, throw an
		 * exception
		 */
		if (streamingInputs.size() == 1
				&& checker.getOperatorContext().getParameterNames()
						.contains("file")) {
			checker.setInvalidContext(
					"HDFSFileSource requires either file parameter to be specified or the number of input ports to be one. Both cannot be specified together.",
					null);
		}
		
	}
	
	@ContextCheck(compile = true)
	public static void checkConsistentRegion(OperatorContextChecker checker) {		
		OperatorContext opContext = checker.getOperatorContext();
		ConsistentRegionContext crContext = opContext.getOptionalContext(ConsistentRegionContext.class);
		if (crContext != null)
		{
			if (crContext.isStartOfRegion() && opContext.getNumberOfStreamingInputs()>0)
			{
				checker.setInvalidContext("The following operator cannot be the start of a consistent region when an input port is present: HDFS2FileSource.", null);
			}
		}
	}

	@ContextCheck(compile = false)
	public static void validateParametersRuntime(OperatorContextChecker checker)
			throws Exception {
		OperatorContext context = checker.getOperatorContext();

		/*
		 * Check if initDelay is negative
		 */
		if (context.getParameterNames().contains("initDelay")) {
			if (Integer.valueOf(context.getParameterValues("initDelay").get(0)) < 0) {
				checker.setInvalidContext(
						"initDelay value {0} should be zero or greater than zero  ",
						new String[] { context.getParameterValues("initDelay")
								.get(0).trim() });
			}
		}
		if (checker.getOperatorContext().getParameterNames().contains(PARALLEL_READ_PARAM) &&
				Boolean.parseBoolean(checker.getOperatorContext().getParameterValues(PARALLEL_READ_PARAM).get(0))) {
			// in parallel mode.
			if (checker.getOperatorContext().getNumberOfStreamingInputs() > 0) {
				checker.setInvalidContext(PARALLEL_READ_PARAM+" cannot be true when number of inputs is greater than zero",new Object[0]);
			}
			if (MetaType.BLOB == checker.getOperatorContext().getStreamingOutputs().get(0).getStreamSchema().getAttribute(0).getType()
					.getMetaType()) {
				checker.setInvalidContext(PARALLEL_READ_PARAM+" may only be used when reading text files.", new Object[0]);
			}
		}
	}

	@ContextCheck(compile = true)
	public static void checkOutputPortSchema(OperatorContextChecker checker)
			throws Exception {
		StreamSchema outputSchema = checker.getOperatorContext()
				.getStreamingOutputs().get(0).getStreamSchema();

		// check that number of attributes is 1
		if (outputSchema.getAttributeCount() != 1) {
			checker.setInvalidContext(
					"Output port can have only one attribute. The type of this attribute has to be rstring, ustring, or blob",
					null);
		}

		if (outputSchema.getAttribute(0).getType().getMetaType() != MetaType.RSTRING
				&& outputSchema.getAttribute(0).getType().getMetaType() != MetaType.USTRING
				&& outputSchema.getAttribute(0).getType().getMetaType() != MetaType.BLOB) {
			checker.setInvalidContext(
					"Expected attribute of type rstring, ustring or blob found attribute of type "
							+ outputSchema.getAttribute(0).getType()
									.getMetaType(), null);
		}

		
		
		if (MetaType.BLOB != outputSchema.getAttribute(0).getType()
				.getMetaType()
				&& checker.getOperatorContext().getParameterNames()
						.contains(BLOCKSIZE_PARAM)) {
			checker.setInvalidContext(
					BLOCKSIZE_PARAM
							+ " may only be used when the outstream an atribute of type blob",
					null);
		}
	}

	@ContextCheck(compile = true)
	public static void checkInputPortSchema(OperatorContextChecker checker)
			throws Exception {
		List<StreamingInput<Tuple>> streamingInputs = checker
				.getOperatorContext().getStreamingInputs();

		// check that we have max of one input port
		if (streamingInputs.size() > 1) {
			throw new Exception("HDFSFileSource can only have one input port");
		}

		// if we have an input port
		if (streamingInputs.size() == 1) {

			StreamSchema inputSchema = checker.getOperatorContext()
					.getStreamingInputs().get(0).getStreamSchema();

			// check that number of attributes is 1
			if (inputSchema.getAttributeCount() != 1) {
				checker.setInvalidContext(
						"Expect only one attribute on input port--attribute should be rstring giving a filename to open",
						null);
			}

			// check that the attribute type must be a rstring
			if (MetaType.RSTRING != inputSchema.getAttribute(0).getType()
					.getMetaType()) {
				checker.setInvalidContext(
						"Expected attribute of type rstring, found attribute of type "
								+ inputSchema.getAttribute(0).getType()
										.getMetaType(), null);
			}
		}
	}

	@ContextCheck(compile = false)
	public static void checkUriMatch(OperatorContextChecker checker)
			throws Exception {
		List<String> hdfsUriParamValues = checker.getOperatorContext()
				.getParameterValues("hdfsUri");
		List<String> fileParamValues = checker.getOperatorContext()
				.getParameterValues("file");

		String hdfsUriValue = null;
		if (hdfsUriParamValues.size() == 1)
			hdfsUriValue = hdfsUriParamValues.get(0);

		String fileValue = null;
		if (fileParamValues.size() == 1)
			fileValue = fileParamValues.get(0);

		// only need to perform this check if both 'hdfsUri' and 'file' params
		// are set
		if (hdfsUriValue != null && fileValue != null) {
			URI hdfsUri;
			URI fileUri;
			try {
				hdfsUri = new URI(hdfsUriValue);
			} catch (URISyntaxException e) {
				LOGGER.log(TraceLevel.ERROR,
						"'hdfsUri' parameter contains an invalid URI: "
								+ hdfsUriValue);
				throw e;
			}

			try {
				fileUri = new URI(fileValue);
			} catch (URISyntaxException e) {
				LOGGER.log(TraceLevel.ERROR,
						"'file' parameter contains an invalid URI: "
								+ fileValue);
				throw e;
			}

			if (fileUri.getScheme() != null) {
				// must have the same scheme
				if (!hdfsUri.getScheme().equals(fileUri.getScheme())) {
					checker.setInvalidContext(
							"The 'file' scheme (" + fileUri.getScheme()
									+ ") must match the 'hdfsUri' scheme ("
									+ hdfsUri.getScheme() + ")", null);
					return;
				}

				// must have the same authority
				if ((hdfsUri.getAuthority() == null && fileUri.getAuthority() != null)
						|| (hdfsUri.getAuthority() != null && fileUri
								.getAuthority() == null)
						|| (hdfsUri.getAuthority() != null
								&& fileUri.getAuthority() != null && !hdfsUri
								.getAuthority().equals(fileUri.getAuthority()))) {
					checker.setInvalidContext(
							"The host and port specified by the 'file' parameter ("
									+ fileUri.getAuthority()
									+ ") must match the host and port specified by the 'hdfsUri' parameter ("
									+ hdfsUri.getAuthority() + ")", null);
					return;
				}
			}
		}
	}

	private void initMetrics(OperatorContext context) {
		nFilesOpened = context.getMetrics()
				.getCustomMetric(FILES_OPENED_METRIC);
	}

	private void processFile(String filename) throws Exception {
		if (LOGGER.isLoggable(LogLevel.INFO)) {
			LOGGER.log(LogLevel.INFO, "Process File: " + filename);
		}
		IHdfsClient hdfsClient = getHdfsClient();
		
		try {
			if (fCrContext != null) {
				fCrContext.acquirePermit();
			}
			openFile(hdfsClient, filename);

		} finally {
			if (fCrContext != null) {
				fCrContext.releasePermit();
			}
		}
		
		if (fDataStream == null) {
			LOGGER.log(LogLevel.ERROR, "Problem opening file " + filename);
			return;
		}
		
		nFilesOpened.incrementValue(1);
		StreamingOutput<OutputTuple> outputPort = getOutput(0);
		try {
			if (fBinaryFile) {
				doReadBinaryFile(fDataStream, outputPort);
			}  else if (parallelRead) {
				doReadFileSplit(fDataStream,outputPort, filename);
			} else {
				doReadTextFile(fDataStream, outputPort, filename);
			}
		} catch (IOException e) {
			LOGGER.log(LogLevel.ERROR,
					"Exception occured during read: " + e.getMessage());
		} finally {
			closeFile();
		}
		outputPort.punctuate(Punctuation.WINDOW_MARKER);
		
		if (fCrContext != null && fCrContext.isStartOfRegion() && fCrContext.isTriggerOperator())
		{
			try 
			{
				fCrContext.acquirePermit();					
				fCrContext.makeConsistent();
			}
			finally {
				fCrContext.releasePermit();
			}
		}
	}

	private void closeFile() throws IOException {
		if (fDataStream != null)
			fDataStream.close();
	}

	private InputStream openFile(IHdfsClient hdfsClient, String filename)
			throws IOException {
		
		if (filename != null)
			fDataStream = hdfsClient.getInputStream(filename);
		
		// reset counter every time we open a file
		fLineNum = 0;
		return fDataStream;
	}

	private void doReadFileSplit(InputStream dataStream,
			StreamingOutput<OutputTuple> outputPort, String filename) 
		throws UnsupportedEncodingException,IOException,Exception {
		IHdfsClient hdfsClient = getHdfsClient();
		long len = hdfsClient.getFileSize(filename);
		long myStart;
		long myEnd;
		int channel = getOperatorContext().getChannel();
		if (getOperatorContext().getChannel() >= 0) {
			long chunkSize = len/getOperatorContext().getMaxChannels(); 
			myStart = chunkSize*getOperatorContext().getChannel();
			if (channel ==  getOperatorContext().getMaxChannels() -1 ) {
				// we are the last channel
			    myEnd = myStart + chunkSize+getOperatorContext().getMaxChannels();
			}
			else {
				myEnd = myStart + chunkSize;
			}
		}
		else {
			myStart = 0;
			myEnd = len +1;
		}
		bufferedLineReader = new BufferedLineReader(dataStream,myStart,myEnd,fEncoding);
		boolean done = false;
		while (!done) {
			try {
				if (fCrContext != null) {
					fCrContext.acquirePermit();
				}
				String line = bufferedLineReader.readLine();
				if (line != null) {
					OutputTuple outputTuple = outputPort.newTuple();
					outputTuple.setString(0, line);
					outputPort.submit(outputTuple);
				}
				else {
					done = true;
				}
			}
			finally {
				if (fCrContext != null) fCrContext.releasePermit();
			}
		}
	}
	
	private void doReadTextFile(InputStream dataStream,
			StreamingOutput<OutputTuple> outputPort, String filename)
			throws UnsupportedEncodingException, IOException, Exception {				
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				dataStream, fEncoding), BUFFER_SIZE);			

		String line = null;
		do {
			try {
				
				if (fCrContext != null)
				{
					fCrContext.acquirePermit();
				}
				
				if (fSeekToLine >=0)
				{

					TRACE.info("Process File Seek to position: " + fSeekToLine);					
					
					reader.close();
					closeFile();
					dataStream = openFile(getHdfsClient(), filename);									
									
					// create new reader and start reading at beginning
					reader = new BufferedReader(new InputStreamReader(
							dataStream, fEncoding), BUFFER_SIZE);
					
					for (int i=0; i<fSeekToLine; i++)
					{
						// skip the lines that have already been processed
						reader.readLine();
						fLineNum++;
					}
					
					fSeekToLine = -1;
				}
								
				line = reader.readLine();
				fLineNum ++;				
	
				if (line != null) {
					// submit tuple
					OutputTuple outputTuple = outputPort.newTuple();
					outputTuple.setString(0, line);
					outputPort.submit(outputTuple);
				}
				
			} finally {
				if (fCrContext != null)
				{
					fCrContext.releasePermit();
				}
			}

		} while (line != null);

		reader.close();
	}

	private void doReadBinaryFile(InputStream dataStream,
			StreamingOutput<OutputTuple> outputPort) throws IOException,
			Exception {
		byte myBuffer[] = new byte[fBlockSize];
		
		int numRead = 0;		
		do {			
			try {
				
				if (fCrContext != null)
				{
					fCrContext.acquirePermit();
				}
				
				if (fSeekPosition >=0)
				{
					TRACE.info("reset to position: " + fSeekPosition);
					((FSDataInputStream)dataStream).seek(fSeekPosition);
					fSeekPosition = -1;
				}
				
				numRead = dataStream.read(myBuffer);
				if (numRead > 0)
				{
					OutputTuple toSend = outputPort.newTuple();
					toSend.setBlob(0, ValueFactory.newBlob(myBuffer, 0, numRead));
					outputPort.submit(toSend);
				}	
			}
			finally {
				if (fCrContext != null)
				{
					fCrContext.releasePermit();
				}
			}			
		} while (numRead > 0);				
	}

	// called on background thread
	protected void process() throws Exception {
		
		fProcessThreadDone = false;
		if (fInitDelay > 0) {
			try {
				Thread.sleep((long) (fInitDelay * 1000));
			} catch (InterruptedException e) {
				LOGGER.log(LogLevel.INFO, "Init delay interrupted");
			}
		}
		try {
			if (!shutdownRequested) {
				processFile(fFileName);
			}
		}finally {
			fProcessThreadDone = true;
		}
	}

	@Override
	public void process(StreamingInput<Tuple> stream, Tuple tuple)
			throws Exception {
		if (shutdownRequested)
			return;
		if (fIsFirstTuple && fInitDelay > 0) {
			try {
				Thread.sleep((long) (fInitDelay * 1000));
			} catch (InterruptedException e) {
				LOGGER.log(LogLevel.INFO, "Init delay interrupted");
			}
		}
		fIsFirstTuple = false;
		String filename = tuple.getString(0);

		// check if file name is an empty string. If so, log a warning and
		// continue with the next tuple
		if (filename.isEmpty()) {
			LOGGER.log(LogLevel.WARN, "file name is an empty string. Skipping.");
		} else {
			// IHdfsClient hdfsClient = getHdfsClient();
			try {
				// hdfsClient.getInputStream(filename);
				processFile(filename);
			} catch (IOException ioException) {
				LOGGER.log(LogLevel.WARN, ioException.getMessage());
			}
		}
	}

	@Parameter(optional = true)
	public void setFile(String file) {
		this.fFileName = file;
	}

	@Parameter(optional = true)
	public void setInitDelay(double initDelay) {
		this.fInitDelay = initDelay;
	}

	@Parameter(optional = true)
	public void setEncoding(String encoding) {
		this.fEncoding = encoding;
	}
	
	@Parameter(name=BLOCKSIZE_PARAM,optional=true)
	public void setBlockSize (int inBlockSize) {
		fBlockSize = inBlockSize;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {		
		
		TRACE.info("Checkpoint " + checkpoint.getSequenceId());
		
		if (!isDynamicFile() && fDataStream instanceof FSDataInputStream)
		{		
			// for binary file
			FSDataInputStream fsDataStream = (FSDataInputStream)fDataStream;
			long pos = fsDataStream.getPos();
			
			TRACE.info("checkpoint position: " + pos);
			
			checkpoint.getOutputStream().writeLong(pos);
			
			// for text file
			TRACE.info("checkpoint lineNumber: " + fLineNum);
			checkpoint.getOutputStream().writeLong(fLineNum);
			if (bufferedLineReader != null) {
				checkpoint.getOutputStream().writeLong(bufferedLineReader.getPos());
			}
			else {
				checkpoint.getOutputStream().writeLong(-1);
			}
		}
	}

	@Override
	public void drain() throws Exception {
		TRACE.info("Drain");
	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		
		TRACE.info("Reset " + checkpoint.getSequenceId());
		
		if (!isDynamicFile())
		{	
			// for binary file
			long pos = checkpoint.getInputStream().readLong();
			fSeekPosition = pos;					
			// for text file
			fSeekToLine = checkpoint.getInputStream().readLong();
			fLineReaderPos = checkpoint.getInputStream().readLong();
			TRACE.info("reset position: " + fSeekPosition);
			TRACE.info("reset lineNumber: " + fSeekToLine);
			
			// if thread is not running anymore, restart thread
			if (fProcessThreadDone)
			{
				TRACE.info("reset process thread");
				processThread = createProcessThread();
				
				// set to false here to avoid the delay of unsetting
				// it inside the process method and end up having multiple threads started
				startProcessing();
			}
		}
	}

	@Override
	public void resetToInitialState() throws Exception {
		
		TRACE.info("Resest to initial");
		if (!isDynamicFile())
		{					
			TRACE.info("Seek to 0");
			fSeekPosition = 0;					
			fSeekToLine = 0;
			
			TRACE.info("reset position: " + fSeekPosition);
			TRACE.info("reset lineNumber: " + fSeekToLine);
			
			// if thread is not running anymore, restart thread
			if (fProcessThreadDone)
			{
				TRACE.info("reset process thread");
				processThread = createProcessThread();
				
				// set to false here to avoid the delay of unsetting
				// it inside the process method and end up having multiple threads started
				startProcessing();
			}
		}
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		// TODO Auto-generated method stub

	}
	
	private boolean isDynamicFile()
	{
		return getOperatorContext().getNumberOfStreamingInputs() > 0;
	}

}

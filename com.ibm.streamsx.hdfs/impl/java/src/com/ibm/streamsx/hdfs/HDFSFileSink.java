/*******************************************************************************
 * Licensed Materials - Property of IBM
 * Copyright IBM Corp. 2014
 * US Government Users Restricted Rights - Use, duplication or
 * disclosure restricted by GSA ADP Schedule Contract with
 * IBM Corp.
 *******************************************************************************/

package com.ibm.streamsx.hdfs;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.SharedLoader;

@SharedLoader
public class HDFSFileSink extends AbstractHdfsOperator {

	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.HDFSFileSink";

	/**
	 * Create a logger specific to this class
	 */
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY
			+ "." + CLASS_NAME, "com.ibm.streamsx.hdfs.BigDataMessages");
	private static Logger TRACE = Logger.getLogger(CLASS_NAME);
	private static final String DYNAMIC_PARAM = "dynamicFilename";
	private static final int FILE_NAME_INDEX = 1;

	private String rawFileName = null;
	private String file = null;
	private String timeFormat = "yyyyMMdd_HHmmss";
	private String currentFileName;

	private HdfsFile fFileToWrite;

	private long bytesPerFile = -1;
	private long tuplesPerFile = -1;
	private double timePerFile = -1;
	private boolean closeOnPunct = false;
	private String encoding = null;
	private boolean dynamicFilename = false;

	// Other variables
	private int fileNum = 0;

	// Variables required by the optional output port
	// hasOutputPort signifies if the operator has output port defined or not
	// assuming in the beginning that the operator does not have a error output
	// port by setting hasOutputPort to false

	private boolean hasOutputPort = false;
	private StreamingOutput<OutputTuple> outputPort;

	private Thread fFileTimerThread;

	/*
	 * The method checkOutputPort validates that the stream on output port
	 * contains the mandatory attribute.
	 */
	@ContextCheck(compile = true)
	public static void checkOutputPort(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		if (context.getNumberOfStreamingOutputs() == 1) {
			StreamingOutput<OutputTuple> streamingOutputPort = context
					.getStreamingOutputs().get(0);
			if (streamingOutputPort.getStreamSchema().getAttributeCount() != 2) {
				checker.setInvalidContext(
						"The optional output port should have two attributes.",
						null);

			} else {
				if (streamingOutputPort.getStreamSchema().getAttribute(0)
						.getType().getMetaType() != Type.MetaType.RSTRING) {
					checker.setInvalidContext(
							"The first attribute in the optional output port must be an rstring.",
							null);
				}
				if (streamingOutputPort.getStreamSchema().getAttribute(1)
						.getType().getMetaType() != Type.MetaType.UINT64) {
					checker.setInvalidContext(
							"The second attribute in the optional output port must be an uint64.",
							null);

				}

			}

		}
	}

	@Parameter(name = DYNAMIC_PARAM, optional = true, description = "If true, the second attribute is taken to be the file name")
	public void setFilenameAttr(boolean changeName) {
		dynamicFilename = changeName;
	}

	// Mandatory parameter file
	@Parameter(optional = true)
	public void setFile(String file) {
		TRACE.log(TraceLevel.DEBUG, "setFile: " + file);
		this.file = file;
	}

	public String getFile() {
		return file;
	}

	public String getCurrentFileName() {
		return currentFileName;
	}

	// Optional parameter timeFormat
	@Parameter(optional = true)
	public void setTimeFormat(String timeFormat) {
		this.timeFormat = timeFormat;
	}

	@Parameter(optional = true)
	public void setBytesPerFile(long bytesPerFile) {
		this.bytesPerFile = bytesPerFile;
	}

	public long getBytesPerFile() {
		return bytesPerFile;
	}

	@Parameter(optional = true)
	public void setTuplesPerFile(long tuplesPerFile) {
		this.tuplesPerFile = tuplesPerFile;
	}

	public long getTuplesPerFile() {
		return tuplesPerFile;
	}

	@Parameter(optional = true)
	public void setTimePerFile(double timePerFile) {
		this.timePerFile = timePerFile;
	}

	public double getTimePerFile() {
		return timePerFile;
	}

	@Parameter(optional = true)
	public void setCloseOnPunct(boolean closeOnPunct) {
		this.closeOnPunct = closeOnPunct;
	}

	public boolean isCloseOnPunct() {
		return closeOnPunct;
	}

	@Parameter(optional = true)
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public String getEncoding() {
		return encoding;
	}

	@ContextCheck(compile = false)
	public static void checkInputPortSchemaRuntime(
			OperatorContextChecker checker) {
		
		StreamSchema inputSchema = checker.getOperatorContext()
				.getStreamingInputs().get(0).getStreamSchema();
		boolean dynamicParam = false;
		if (checker.getOperatorContext().getParameterNames()
		    .contains(DYNAMIC_PARAM)) {
		dynamicParam = Boolean.parseBoolean(checker
						    .getOperatorContext().getParameterValues(DYNAMIC_PARAM).get(0));
		}
		if ((dynamicParam && inputSchema.getAttributeCount() != 2)
				|| (!dynamicParam && inputSchema.getAttributeCount() != 1)) {
			checker.setInvalidContext(
					"Input stream must have one attribute when "
							+ DYNAMIC_PARAM
							+ " is false, and two attributes when "
							+ DYNAMIC_PARAM + " is true", new Object[] {});
		}
		boolean hasFile = checker.getOperatorContext().getParameterNames()
				.contains("file");
		if (!hasFile && !dynamicParam) {
			checker.setInvalidContext("Must have file parameter specified unless "+DYNAMIC_PARAM+" is true",new Object[]{});
		}
	}

	@ContextCheck(compile = true)
	public static void checkInputPortSchema(OperatorContextChecker checker)
			throws Exception {
		// rstring or ustring would need to be provided.
		StreamSchema inputSchema = checker.getOperatorContext()
				.getStreamingInputs().get(0).getStreamSchema();
		boolean hasDynamic = checker.getOperatorContext().getParameterNames()
				.contains(DYNAMIC_PARAM);
		boolean hasFile = checker.getOperatorContext().getParameterNames()
				.contains("file");
		if (!hasFile && !hasDynamic) {
			checker.setInvalidContext("Must have file parameter specified unless "+DYNAMIC_PARAM+" is true",new Object[]{});
		}
		if (!hasDynamic && inputSchema.getAttributeCount() != 1) {
			checker.setInvalidContext(
					"Input stream must have one attribute unless "
							+ DYNAMIC_PARAM + " is specified", new Object[] {});
		}
		if (hasDynamic && inputSchema.getAttributeCount() < 1
				|| inputSchema.getAttributeCount() > 2) {
			checker.setInvalidContext(
					"Input stream must have one attribute when "
							+ DYNAMIC_PARAM
							+ " is false, and two attributes when "
							+ DYNAMIC_PARAM + " is true", new Object[] {});
		}

		// if we have two attributes on the input stream, check that the second
		// is the right type.
		if (hasDynamic && inputSchema.getAttributeCount() == 2) {
			if (MetaType.RSTRING != inputSchema.getAttribute(FILE_NAME_INDEX)
					.getType().getMetaType()) {
				checker.setInvalidContext(
						"Filename attribute must be of type rstring",
						new Object[] {});
			}
		}

		// check that the attribute type must be a rstring or ustring
		if (MetaType.RSTRING != inputSchema.getAttribute(0).getType()
				.getMetaType()
				&& MetaType.USTRING != inputSchema.getAttribute(0).getType()
						.getMetaType()) {
			checker.setInvalidContext(
					"Expected attribute of type rstring or ustring on input port, found attribute of type "
							+ inputSchema.getAttribute(0).getType()
									.getMetaType(), null);
		}
	}

	@ContextCheck(compile = true)
	public static void checkCompileParameters(OperatorContextChecker checker)
			throws Exception {
		checker.checkExcludedParameters(IHdfsConstants.PARAM_BYTES_PER_FILE,
				IHdfsConstants.PARAM_TIME_PER_FILE,
				IHdfsConstants.PARAM_TUPLES_PER_FILE);
		checker.checkExcludedParameters(IHdfsConstants.PARAM_TIME_PER_FILE,
				IHdfsConstants.PARAM_BYTES_PER_FILE,
				IHdfsConstants.PARAM_TUPLES_PER_FILE);
		checker.checkExcludedParameters(IHdfsConstants.PARAM_TUPLES_PER_FILE,
				IHdfsConstants.PARAM_BYTES_PER_FILE,
				IHdfsConstants.PARAM_TIME_PER_FILE);

	}

	@ContextCheck(compile = false)
	public static void checkParameters(OperatorContextChecker checker)
			throws Exception {
		List<String> paramValues = checker.getOperatorContext()
				.getParameterValues("file");
		List<String> timeFormatValue = checker.getOperatorContext()
				.getParameterValues("timeFormat");
		if (timeFormatValue != null) {
			if (!timeFormatValue.isEmpty()) {
				if (timeFormatValue.get(0).isEmpty()) {
					throw new Exception(
							"Operator parameter timeFormat should not be empty");
				}
			}
		}
		for (String fileValue : paramValues) {
			if (fileValue.contains(IHdfsConstants.FILE_VAR_PREFIX)) {
				if (!fileValue.contains(IHdfsConstants.FILE_VAR_HOST)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_PROCID)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_PEID)
						&& !fileValue
								.contains(IHdfsConstants.FILE_VAR_PELAUNCHNUM)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_TIME)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_FILENUM)) {
					throw new Exception(
							"Unsupported % specification provided. Supported values are %HOST, %PEID, %FILENUM, %PROCID, %PELAUNCHNUM, %TIME");
				}
			}
		}

		List<String> bytesPerFileVal = checker.getOperatorContext()
				.getParameterValues(IHdfsConstants.PARAM_BYTES_PER_FILE);
		List<String> tuplesPerFileVal = checker.getOperatorContext()
				.getParameterValues(IHdfsConstants.PARAM_TUPLES_PER_FILE);
		List<String> timeForFileVal = checker.getOperatorContext()
				.getParameterValues(IHdfsConstants.PARAM_TIME_PER_FILE);

		// checks for negative values
		if (!bytesPerFileVal.isEmpty()) {
			if (Integer.valueOf(bytesPerFileVal.get(0)) < 0) {
				checker.setInvalidContext(
						"Operator parameter bytesPerFile value should not be less than 0.",
						null);
			}
		}

		if (!tuplesPerFileVal.isEmpty()) {
			if (Integer.valueOf(tuplesPerFileVal.get(0)) < 0) {
				checker.setInvalidContext(
						"Operator parameter tuplesPerFile value should not be less than 0.",
						null);
			}
		}

		if (!timeForFileVal.isEmpty()) {
			if (Float.valueOf(timeForFileVal.get(0)) < 0.0) {
				checker.setInvalidContext(
						"Operator parameter timePerFile value should not be less than 0.",
						null);
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
		if (fileParamValues.size() == 1) {
			fileValue = fileParamValues.get(0);
		// replace % with _
		fileValue = fileValue.replace("%", "_");
		}
		// only need to perform this check if both 'hdfsUri' and 'file' params
		// are set
		if (hdfsUriValue != null && fileValue != null) {

			// log error message for individual params if invalid URI
			URI hdfsUri;
			URI fileUri;
			try {
				hdfsUri = new URI(hdfsUriValue);
			} catch (URISyntaxException e) {
				TRACE.log(TraceLevel.ERROR,
						"'hdfsUri' parameter contains an invalid URI: "
								+ hdfsUriValue);
				throw e;
			}

			try {
				fileUri = new URI(fileValue);
			} catch (URISyntaxException e) {
				TRACE.log(TraceLevel.ERROR,
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

	@Override
	public void initialize(OperatorContext context) throws Exception {

		try {

			// if the file contains variable, it will result in an
			// URISyntaxException, replace % with _ so we can parse the URI
			TRACE.log(TraceLevel.DEBUG, "file param: " + file);

			/*
			 * Set appropriate variables if the optional output port is
			 * specified. Also set outputPort to the output port at index 0
			 */
			if (context.getNumberOfStreamingOutputs() == 1) {

				hasOutputPort = true;

				outputPort = context.getStreamingOutputs().get(0);

			}
			if (file != null) {
				String fileparam = file;
				fileparam = fileparam.replace("%", "_");

				URI uri = new URI(fileparam);

				TRACE.log(TraceLevel.DEBUG, "uri: " + uri.toString());

				String scheme = uri.getScheme();
				if (scheme != null) {
					String fs;
					if (uri.getAuthority() != null)
						fs = scheme + "://" + uri.getAuthority();
					else
						fs = scheme + ":///";

					// only use the authority from the 'file' parameter if the
					// 'hdfsUri' param is not specified
					if (getHdfsUri() == null)
						setHdfsUri(fs);

					TRACE.log(TraceLevel.DEBUG, "fileSystemUri: "
							+ getHdfsUri());

					// must use original parameter value to preserve the
					// variable
					String path = file.substring(fs.length());

					// since the file contains a scheme, the path is absolute
					// and we
					// need to ensure it starts a "/"
					if (!path.startsWith("/"))
						path = "/" + path;

					setFile(path);
				}
			}
		} catch (URISyntaxException e) {

			TRACE.log(TraceLevel.DEBUG,
					"Unable to construct URI: " + e.getMessage());

			throw e;
		}

		super.initialize(context);

		if (!dynamicFilename) {
			refreshCurrentFileName(file);
			createFile();
		}
		processThread = createProcessThread();
	}

	private void createFile() {
		fFileToWrite = new HdfsFile(getOperatorContext(), getCurrentFileName(),
				getHdfsClient(), getEncoding());
		if (getTimePerFile() > 0) {
			fFileToWrite.setExpPolicy(EnumFileExpirationPolicy.TIME);
			// time in parameter specified in seconds, need to convert to
			// miliseconds
			fFileToWrite.setTimePerFile(getTimePerFile() * 1000);
			createFileTimer(getTimePerFile() * 1000, fFileToWrite);

		} else if (getBytesPerFile() > 0) {
			fFileToWrite.setExpPolicy(EnumFileExpirationPolicy.SIZE);
			fFileToWrite.setSizePerFile(getBytesPerFile());
		} else if (getTuplesPerFile() > 0) {
			fFileToWrite.setExpPolicy(EnumFileExpirationPolicy.TUPLECNT);
			fFileToWrite.setTuplesPerFile(getTuplesPerFile());
		}
	}

	private void createFileTimer(final double time, final HdfsFile file) {

		// This thread handles the timePerFile expiration policy
		// This thread sleeps for the time specified.
		// When it wakes up, it will set the file as expired and close it
		// When the next tuple comes in, we check that the file has
		// expired and will create a new file for writing
		fFileTimerThread = getOperatorContext().getThreadFactory().newThread(
				new Runnable() {

					@Override
					public void run() {
						try {
							TRACE.log(TraceLevel.DEBUG, "File Timer Started: "
									+ time);
							Thread.sleep((long) time);
							fileTimerExpired(file);
						} catch (Exception e) {
							TRACE.log(TraceLevel.DEBUG,
									"Exception in file timer thread.", e);
						}
					}
				});
		fFileTimerThread.setDaemon(false);
		fFileTimerThread.start();
	}

	private void refreshCurrentFileName(String baseName)
			throws UnknownHostException {

		// We must preserve the file parameter in order for us
		// to support multi-file in the operator

		// Check if % specification mentioned are valid or not
		currentFileName = baseName;
		if (currentFileName.contains(IHdfsConstants.FILE_VAR_PREFIX)) {
			// Replace % specifications with relevant values.
			currentFileName = currentFileName.replace(
					IHdfsConstants.FILE_VAR_HOST, InetAddress.getLocalHost()
							.getHostName());
			currentFileName = currentFileName.replace(
					IHdfsConstants.FILE_VAR_PROCID, ManagementFactory
							.getRuntimeMXBean().getName());
			currentFileName = currentFileName.replace(
					IHdfsConstants.FILE_VAR_PEID, getOperatorContext().getPE()
							.getPEId().toString());
			currentFileName = currentFileName.replace(
					IHdfsConstants.FILE_VAR_PELAUNCHNUM, String
							.valueOf(getOperatorContext().getPE()
									.getRelaunchCount()));
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
			currentFileName = currentFileName.replace(
					IHdfsConstants.FILE_VAR_TIME, sdf.format(cal.getTime()));

			if (fFileToWrite != null && fFileToWrite.numTuples == 0) {
				int num = fileNum - 1; // we want to use the previous value
				currentFileName = currentFileName.replace(
						IHdfsConstants.FILE_VAR_FILENUM, String.valueOf(num));
			} else {
				currentFileName = currentFileName.replace(
						IHdfsConstants.FILE_VAR_FILENUM,
						String.valueOf(getNextFileNum()));
			}

		}
	}

	public int getNextFileNum() {
		return fileNum++;
	}

	@Override
	public void processPunctuation(StreamingInput<Tuple> arg0, Punctuation punct)
			throws Exception {

		TRACE.log(TraceLevel.DEBUG, "Punctuation Received.");

		if (punct == Punctuation.FINAL_MARKER) {
			TRACE.log(TraceLevel.DEBUG, "Final Punctuation, close file.");
			// If Optional output port is present and neither the closeOnPunt is
			// true and other param
			// bytesPerFile and tuplesPerFile is also not set then output the
			// filename and file
			// size
			closeFile();
		}
		// set the file to expire after punctuation
		// on the next write, the file will be recreated
		else if (isCloseOnPunct() && fFileToWrite != null) {

			// This handles the closeOnPunct expiration policy
			TRACE.log(TraceLevel.DEBUG, "Close on punct, close file.");

			closeFile();

		}

	}

	private void closeFile() throws Exception {

		TRACE.log(TraceLevel.DEBUG, "Close File");

		// stop the timer thread.. and create a new one when a new file is
		// created.
		if (fFileTimerThread != null) {
			// interrupt the thread if we are not on the same thread
			// otherwise, keep it going...
			if (Thread.currentThread() != fFileTimerThread) {
				TRACE.log(TraceLevel.DEBUG, "Stop file timer thread");
				fFileTimerThread.interrupt();
			}

			fFileTimerThread = null;
		}

		TRACE.log(TraceLevel.DEBUG, "Set file as expired");

		// If Optional output port is present output the filename and file
		// size

		synchronized (this) {
			if (hasOutputPort && !fFileToWrite.isExpired()) {
				submitOnOutputPort(getCurrentFileName(), fFileToWrite.getSize());
			}
			fFileToWrite.setExpired();

			fFileToWrite.close();
		}

	}

	@Override
	synchronized public void process(StreamingInput<Tuple> stream, Tuple tuple)
			throws Exception {

		if (dynamicFilename) {
			String filenameString = tuple.getString(FILE_NAME_INDEX);
			if (rawFileName == null) {
				// the first tuple. No raw file name is set.
				rawFileName = filenameString;
				refreshCurrentFileName(rawFileName);
				createFile();
			}

			if (!rawFileName.equals(filenameString)) {
				// the filename has changed. Notice this cannot happen on the
				// first tuple.
				closeFile();
				rawFileName = filenameString;
				refreshCurrentFileName(rawFileName);
				createFile();
			}
			// When we leave this block, we know the file is ready to be written
			// to.
		}

		if (fFileToWrite != null) {

			if (fFileToWrite.isExpired()) {
				// if the file is expired, , the file would have been closed
				// create a new file and start writing from that instead
				refreshCurrentFileName(file);
				createFile();
			}

			fFileToWrite.writeTuple(tuple);
			// This will check bytesPerFile and tuplesPerFile expiration policy
			if (fFileToWrite.isExpired()) {
				// If Optional output port is present output the filename and
				// file size
				if (hasOutputPort) {
					submitOnOutputPort(getCurrentFileName(),
							fFileToWrite.getSize());
				}
				fFileToWrite.close();
			}

		}

	}

	private void submitOnOutputPort(String filename, long size)
			throws Exception {

		if (TRACE.isLoggable(TraceLevel.DEBUG))
			TRACE.log(TraceLevel.DEBUG,
					"Submit filename and size on output port: " + filename
							+ " " + size);

		OutputTuple outputTuple = outputPort.newTuple();

		outputTuple.setString(0, filename);
		outputTuple.setLong(1, size);

		outputPort.submit(outputTuple);
	}

	@Override
	public void shutdown() throws Exception {
		if (fFileToWrite != null) {
			fFileToWrite.close();
			fFileToWrite = null;
		}

		if (fFileTimerThread != null) {
			fFileTimerThread.interrupt();
		}
		super.shutdown();
	}

	private void fileTimerExpired(final HdfsFile file) throws Exception {

		if (TRACE.isLoggable(TraceLevel.DEBUG))
			TRACE.log(TraceLevel.DEBUG, "File Timer Expired: " + file);

		// when the timer wakes up and we still have the same file,
		// mark the file as expired
		if (fFileToWrite == file) {
			TRACE.log(TraceLevel.DEBUG, "File Timer Expired, close file");

			closeFile();
		}
	}
}

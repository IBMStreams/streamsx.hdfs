/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
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
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streamsx.hdfs.client.IHdfsClient;

@SharedLoader
public class HDFSFileSource extends AbstractHdfsOperator {

	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.HDFSFileSource";

	private static Logger logger = Logger.getLogger(LoggerNames.LOG_FACILITY
			+ "." + CLASS_NAME, "com.ibm.streamsx.hdfs.BigDataMessages");
	
	private static Logger trace = Logger.getLogger(HDFSFileSource.class.getName());
			 
	// TODO check that name matches filesource change required
	private final String FILES_OPENED_METRIC = "nFilesOpened";

	private Metric nFilesOpened;

	private String file;
	private double initDelay;

	boolean isFirstTuple = true;
	boolean binaryFile = false;
	int blockSize = 1024 * 4;

	private String encoding = "UTF-8";

	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		if (file != null) {
			try {
				URI uri = new URI(file);
				logger.log(TraceLevel.DEBUG, "uri: " + uri.toString());

				String scheme = uri.getScheme();
				if (scheme != null) {
					String fs;
					if (uri.getAuthority() != null)
						fs = scheme + "://" + uri.getAuthority();
					else
						fs = scheme + ":///";

					if (getHdfsUri() == null)
						setHdfsUri(fs);

					logger.log(TraceLevel.DEBUG, "fileSystemUri: "
							+ getHdfsUri());

					// Use original parameter value
					String path = file.substring(fs.length());

					if (!path.startsWith("/"))
						path = "/" + path;

					setFile(path);
				}
			} catch (URISyntaxException e) {
				logger.log(TraceLevel.DEBUG,
						"Unable to construct URI: " + e.getMessage());

				throw e;
			}
		}

		super.initialize(context);

		// Associate the aspect Log with messages from the SPL log
		// logger.
		setLoggerAspects(logger.getName(), "HDFSFileSource");

		initMetrics(context);

		if (file != null) {
			processThread = createProcessThread();
		}
		
		StreamSchema outputSchema =context.getStreamingOutputs().get(0).getStreamSchema();
		MetaType outType = outputSchema.getAttribute(0).getType().getMetaType();
		if (MetaType.BLOB == outType) {
			binaryFile = true;
			trace.info("File will be read as a binary blobs of size "+blockSize);
		}
		else {
			trace.info("Files will be read as text files, with one tuple per line.");
		}
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
				&& outputSchema.getAttribute(0).getType().getMetaType() != MetaType.USTRING &&
				outputSchema.getAttribute(0).getType().getMetaType() != MetaType.BLOB) {
			checker.setInvalidContext(
					"Expected attribute of type rstring, ustring or blob found attribute of type "
							+ outputSchema.getAttribute(0).getType()
									.getMetaType(), null);
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
				logger.log(TraceLevel.ERROR,
						"'hdfsUri' parameter contains an invalid URI: "
								+ hdfsUriValue);
				throw e;
			}

			try {
				fileUri = new URI(fileValue);
			} catch (URISyntaxException e) {
				logger.log(TraceLevel.ERROR,
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

		if (logger.isLoggable(LogLevel.INFO)) {
			logger.log(LogLevel.INFO, "Process File: " + filename);
		}

		IHdfsClient hdfsClient = getHdfsClient();

		InputStream dataStream = hdfsClient.getInputStream(filename);
		if (dataStream == null) {
			logger.log(LogLevel.ERROR, "Problem opening file " + filename);
			return;
		}
		nFilesOpened.incrementValue(1);

		StreamingOutput<OutputTuple> outputPort = getOutput(0);

		try {
			if (binaryFile) {
				byte myBuffer[] = new byte[blockSize];
				int num_read = dataStream.read(myBuffer);
				while (num_read > 0) {
					// TODO talk with dan about what's most efficient
					OutputTuple toSend = outputPort.newTuple();
					toSend.setBlob(0,
							ValueFactory.newBlob(myBuffer, 0, num_read));
					outputPort.submit(toSend);
					num_read = dataStream.read(myBuffer);
				}
			} else {
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(dataStream, encoding),
						1024 * 1024 * 8);
				String line = reader.readLine();

				while (line != null) {

					// submit tuple
					OutputTuple outputTuple = outputPort.newTuple();
					outputTuple.setString(0, line);
					outputPort.submit(outputTuple);

					// read next line
					line = reader.readLine();
				}			
				reader.close();	
			}
		} catch (IOException e) {
			logger.log(LogLevel.ERROR,
					"Exception occured during read: " + e.getMessage());
		} finally {
			dataStream.close();
		}
		outputPort.punctuate(Punctuation.WINDOW_MARKER);
	}

	protected void process() throws Exception {
		if (initDelay > 0) {
			try {
				Thread.sleep((long) (initDelay * 1000));
			} catch (InterruptedException e) {
				logger.log(LogLevel.INFO, "Init delay interrupted");
			}
		}
		if (!shutdownRequested) {
			processFile(file);
		}
	}

	@Override
	public void process(StreamingInput<Tuple> stream, Tuple tuple)
			throws Exception {
		if (shutdownRequested)
			return;
		if (isFirstTuple && initDelay > 0) {
			try {
				Thread.sleep((long) (initDelay * 1000));
			} catch (InterruptedException e) {
				logger.log(LogLevel.INFO, "Init delay interrupted");
			}
		}
		isFirstTuple = false;
		String filename = tuple.getString(0);

		// check if file name is an empty string. If so, log a warning and
		// continue with the next tuple
		if (filename.isEmpty()) {
			logger.log(LogLevel.WARN, "file name is an empty string. Skipping.");
		} else {
			// IHdfsClient hdfsClient = getHdfsClient();
			try {
				// hdfsClient.getInputStream(filename);
				processFile(filename);
			} catch (IOException ioException) {
				logger.log(LogLevel.WARN, ioException.getMessage());
			}
		}

		// TODO: Process file already submits a marker, why do we need to do
		// this here?
		// getOutput(0).punctuate(Punctuation.WINDOW_MARKER);
	}

	@Parameter(optional = true)
	public void setFile(String file) {
		this.file = file;
	}

	@Parameter(optional = true)
	public void setInitDelay(double initDelay) {
		this.initDelay = initDelay;
	}

	@Parameter(optional = true)
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

}

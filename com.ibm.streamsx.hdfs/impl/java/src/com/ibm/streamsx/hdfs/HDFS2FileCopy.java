/*******************************************************************************
 * Copyright (C) 2014-2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;

@PrimitiveOperator(name = "HDFS2FileCopy", namespace = "com.ibm.streamsx.hdfs", description = IHdfsConstants.DESC_HDFS_FILE_COPY)

@Icons(location32 = "impl/java/icons/HDFS2FileCopy_32.gif", location16 = "impl/java/icons/HDFS2FileCopy_16.gif")

@InputPorts({
		@InputPortSet(description = IHdfsConstants.DESC_HDFS_FILE_COPY_INPUT, cardinality = 1, optional = true, controlPort = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })

@OutputPorts({
		@OutputPortSet(description = IHdfsConstants.DESC_HDFS_FILE_COPY_OUTPUT, cardinality = 1, optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Free) })

@SharedLoader
public class HDFS2FileCopy extends AbstractHdfsOperator implements StateHandler {

	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.HDFS2FileCopy";
	private static final String CONSISTEN_ASPECT = "com.ibm.streamsx.hdfs.HDFS2FileCopy.consistent";

	/**
	 * Create a logger specific to this class
	 */
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME);

	private static Logger TRACE = Logger.getLogger(CLASS_NAME);

	private String localFileAttrName = null;
	private String hdfsFileAttrName = null;
	private String localFile = null;
	private String hdfsFile = null;
	private boolean deleteSourceFile = false;
	private boolean overwriteDestinationFile = false;

	public enum copyDirection {
		copyFromLocalFile, copyToLocalFile
	}

	private copyDirection direction = copyDirection.copyFromLocalFile;

	// Variables required by the optional output port
	// hasOutputPort signifies if the operator has output port defined or not
	// assuming in the beginning that the operator does not have a error output
	// port by setting hasOutputPort to false

	private boolean hasOutputPort = false;
	private StreamingOutput<OutputTuple> outputPort;

	private LinkedBlockingQueue<OutputTuple> outputPortQueue;
	private Thread outputPortThread;

	private boolean isRestarting;
	private ConsistentRegionContext crContext;

	/*
	 * The method checkOutputPort validates that the stream on output port
	 * contains the mandatory attribute.
	 */
	@ContextCheck(compile = true)
	public static void checkOutputPort(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		if (context.getNumberOfStreamingOutputs() == 1) {
			StreamingOutput<OutputTuple> streamingOutputPort = context.getStreamingOutputs().get(0);
			if (streamingOutputPort.getStreamSchema().getAttributeCount() != 2) {
				checker.setInvalidContext(Messages.getString("HDFS_SINK_OUTPUT_PORT"), null);

			} else {
				if (streamingOutputPort.getStreamSchema().getAttribute(0).getType()
						.getMetaType() != Type.MetaType.RSTRING) {
					checker.setInvalidContext(Messages.getString("HDFS_SINK_FIRST_OUTPUT_PORT"), null);
				}
				if (streamingOutputPort.getStreamSchema().getAttribute(1).getType()
						.getMetaType() != Type.MetaType.UINT64) {
					checker.setInvalidContext(Messages.getString("HDFS_SINK_SECOND_OUTPUT_PORT"), null);

				}

			}

		}
	}

	@Parameter(name = IHdfsConstants.PARAM_LOCAL_FILE_NAME, optional = true, description = IHdfsConstants.DESC_HDFS_COPYY_LOCAL_FILE)
	public void setLocalFile(String localFile) {
		this.localFile = localFile;
	}

	@Parameter(name = IHdfsConstants.PARAM_HDFS_FILE_NAME, optional = true, description = IHdfsConstants.DESC_HDFS_COPYY_HDFS_FILE)
	public void setHdfsFile(String hdfsFile) {
		this.hdfsFile = hdfsFile;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_HDFS_COPYY_DELETE_SOURCE_FILE)
	public void setDeleteSourceFile(boolean deleteSourceFile) {
		this.deleteSourceFile = deleteSourceFile;
	}

	@Parameter(optional = true, description = IHdfsConstants.DESC_HDFS_COPYY_OVERWRITE_DEST_FILE)
	public void setOverwriteDestinationFile(boolean overwriteDestinationFile) {
		this.overwriteDestinationFile = overwriteDestinationFile;
	}

	@Parameter(optional = false, description = IHdfsConstants.DESC_HDFS_COPYY_DIRECTION)
	public void setDirection(copyDirection direction) {
		this.direction = direction;
	}

	@Parameter(name = IHdfsConstants.PARAM_LOCAL_FILE_NAME_ATTR, optional = true, description = IHdfsConstants.DESC_HDFS_COPYY_LOCAL_ATTR_FILE)
	public void setLocalFileAttrName(String localFileAttrName) {
		this.localFileAttrName = localFileAttrName;
	}

	@Parameter(name = IHdfsConstants.PARAM_HDFS_FILE_NAME_ATTR, optional = true, description = IHdfsConstants.DESC_HDFS_COPYY_HDFS_ATTR_FILE)
	public void setHdfsFileAttrName(String hdfsFileAttrName) {
		this.hdfsFileAttrName = hdfsFileAttrName;
	}

	/**
	 * This function checks only things that can be determined at compile time.
	 * 
	 * @param checker
	 * @throws Exception
	 */
	@ContextCheck(compile = true)
	public static void checkInputPortSchema(OperatorContextChecker checker) throws Exception {
		// rstring would need to be provided.
		StreamSchema inputSchema = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema();
		if (inputSchema.getAttributeCount() > 0) {
			MetaType metaType = inputSchema.getAttribute(0).getType().getMetaType();
			if (MetaType.RSTRING != metaType) {
				checker.setInvalidContext(Messages.getString("HDFS_DS_INVALID_ATTRIBUTE", metaType), new Object[] {});
			}
		}
		if (inputSchema.getAttributeCount() > 1) {
			MetaType metaType = inputSchema.getAttribute(1).getType().getMetaType();
			if (MetaType.RSTRING != metaType) {
				checker.setInvalidContext(Messages.getString("HDFS_DS_INVALID_ATTRIBUTE", metaType), new Object[] {});
			}
		}

	}

	@ContextCheck(compile = true)
	public static void checkCompileParameters(OperatorContextChecker checker) throws Exception {
		OperatorContext context = checker.getOperatorContext();
		checker.checkExcludedParameters(IHdfsConstants.PARAM_LOCAL_FILE_NAME,
				IHdfsConstants.PARAM_LOCAL_FILE_NAME_ATTR);
		checker.checkExcludedParameters(IHdfsConstants.PARAM_HDFS_FILE_NAME_ATTR, IHdfsConstants.PARAM_HDFS_FILE_NAME);

		if ((!context.getParameterNames().contains(IHdfsConstants.PARAM_LOCAL_FILE_NAME)) && (!context
				.getParameterNames().contains(IHdfsConstants.PARAM_LOCAL_FILE_NAME_ATTR))) {
			checker.setInvalidContext("One of these parameters must be set in opeartor: '"
					+ IHdfsConstants.PARAM_LOCAL_FILE_NAME + "' or '" + IHdfsConstants.PARAM_LOCAL_FILE_NAME_ATTR + "'",
					null);
		}

		if ((!context.getParameterNames().contains(IHdfsConstants.PARAM_HDFS_FILE_NAME)) && (!context
				.getParameterNames().contains(IHdfsConstants.PARAM_HDFS_FILE_NAME_ATTR))) {
			checker.setInvalidContext("One of these parameters must be set in opeartor: '"
					+ IHdfsConstants.PARAM_HDFS_FILE_NAME + "' or '" + IHdfsConstants.PARAM_HDFS_FILE_NAME_ATTR + "'",
					null);
		}
	}

	@ContextCheck(compile = true)
	public static void checkConsistentRegion(OperatorContextChecker checker) {

		// check that the file copy is not at the start of the consistent region
		OperatorContext opContext = checker.getOperatorContext();
		ConsistentRegionContext crContext = opContext.getOptionalContext(ConsistentRegionContext.class);
		if (crContext != null) {
			if (crContext.isStartOfRegion()) {
				checker.setInvalidContext(Messages.getString("HDFS_NOT_CONSISTENT_REGION", "HDFS2FileCopy"), null);
			}
			// check that tempFile parameter is not used in consistent region
			Set<String> parameters = opContext.getParameterNames();
			if (parameters.contains("tempFile")) {
				checker.setInvalidContext(Messages.getString("HDFS_SINK_INVALID_PARAM_TEMPFILE", "HDFS2FileCopy"),
						null);
			}
		}
	}


	/**
	 * Check that the fileAttributeName parameter is an attribute of the right
	 * type.
	 * 
	 * @param checker
	 */
	@ContextCheck(compile = false)
	public static void checkFileAttributeName(OperatorContextChecker checker) {
		StreamSchema inputSchema = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema();
		List<String> localFileAttrNameList = checker.getOperatorContext().getParameterValues(
				IHdfsConstants.PARAM_LOCAL_FILE_NAME_ATTR);
		if (localFileAttrNameList != null && localFileAttrNameList.size() > 0) {
			String localFileAttrName = localFileAttrNameList.get(0);
			Attribute localFileAttr = inputSchema.getAttribute(localFileAttrName);
			if (localFileAttr == null) {
				checker.setInvalidContext(Messages.getString("HDFS_SINK_NO_ATTRIBUTE"), new Object[] {
						localFileAttrName });
			} else if (MetaType.RSTRING != localFileAttr.getType().getMetaType() && MetaType.USTRING != localFileAttr.getType()
					.getMetaType()) {
				checker.setInvalidContext(Messages.getString("HDFS_SINK_INVALID_ATTR_FILENAME", localFileAttr.getType()
						.getMetaType()), new Object[] {});
			}
		}
		List<String> hdfsFileAttrNameList = checker.getOperatorContext().getParameterValues(
				IHdfsConstants.PARAM_HDFS_FILE_NAME_ATTR);
		if (hdfsFileAttrNameList != null && hdfsFileAttrNameList.size() > 0) {
			String hdfsFileAttrName = hdfsFileAttrNameList.get(0);
			Attribute hdfsFileAttr = inputSchema.getAttribute(hdfsFileAttrName);
			if (hdfsFileAttr == null) {
				checker.setInvalidContext(Messages.getString("HDFS_SINK_NO_ATTRIBUTE"), new Object[] {
						hdfsFileAttrName });
			} else if (MetaType.RSTRING != hdfsFileAttr.getType().getMetaType() && MetaType.USTRING != hdfsFileAttr.getType()
					.getMetaType()) {
				checker.setInvalidContext(Messages.getString("HDFS_SINK_INVALID_ATTR_FILENAME", hdfsFileAttr.getType()
						.getMetaType()), new Object[] {});
			}
		}

	}

	@ContextCheck(compile = false)
	public static void checkUriMatch(OperatorContextChecker checker) throws Exception {
		List<String> hdfsUriParamValues = checker.getOperatorContext().getParameterValues("hdfsUri");
		List<String> fileParamValues = checker.getOperatorContext().getParameterValues("file");

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
				TRACE.log(TraceLevel.ERROR, "'hdfsUri' parameter contains an invalid URI: " + hdfsUriValue);
				throw e;
			}

			try {
				fileUri = new URI(fileValue);
			} catch (URISyntaxException e) {
				TRACE.log(TraceLevel.ERROR, "'file' parameter contains an invalid URI: " + fileValue);
				throw e;
			}

			if (fileUri.getScheme() != null) {
				// must have the same scheme
				if (!hdfsUri.getScheme().equals(fileUri.getScheme())) {
					checker.setInvalidContext(Messages.getString("HDFS_SINK_INVALID_SCHEMA", fileUri.getScheme(),
							hdfsUri.getScheme()), null);
					return;
				}

				// must have the same authority
				if ((hdfsUri.getAuthority() == null && fileUri.getAuthority() != null) || (hdfsUri
						.getAuthority() != null && fileUri.getAuthority() == null) || (hdfsUri.getAuthority() != null
								&& fileUri.getAuthority() != null && !hdfsUri.getAuthority().equals(fileUri
										.getAuthority()))) {
					checker.setInvalidContext(Messages.getString("HDFS_SINK_INVALID_HOST", fileUri.getAuthority(),
							hdfsUri.getAuthority()), null);
					return;
				}
			}
		}
	}

	@Override
	public void initialize(OperatorContext context) throws Exception {

		try {
			TRACE.log(TraceLevel.DEBUG, "file param: " + localFile);

			crContext = context.getOptionalContext(ConsistentRegionContext.class);

			super.initialize(context);

			/*
			 * Set appropriate variables if the optional output port is
			 * specified. Also set outputPort to the output port at index 0
			 */
			if (context.getNumberOfStreamingOutputs() == 1) {
				hasOutputPort = true;
				outputPort = context.getStreamingOutputs().get(0);
			}
		} catch (URISyntaxException e) {
			TRACE.log(TraceLevel.DEBUG, "Unable to construct URI: " + e.getMessage());
			throw e;
		}

	}

	@Override
	synchronized public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {

		String message = "";
		String hdfsFullPath = hdfsFile;
		String localFileName = "";
		String hdfsFileNmae = "";
		// get the start time
		long millisStart = Calendar.getInstance().getTimeInMillis();
		try {
			if (localFileAttrName != null) {
				localFile = tuple.getString(localFileAttrName);
			}

			if (hdfsFileAttrName != null) {
				hdfsFile = tuple.getString(hdfsFileAttrName);
			}

			if ((localFile != null) && (hdfsFile != null)) {
				if (fs != null) {
					if (!localFile.startsWith(File.separator)) {
						localFile = getOperatorContext().getPE().getDataDirectory() + File.separator + localFile;
					}

					if (direction == copyDirection.copyFromLocalFile) {
						localFileName = localFile.substring(localFile.lastIndexOf(File.separator) + 1);
						if (hdfsFile.endsWith(File.separator)) {
							hdfsFileNmae = hdfsFile + localFileName;
						} else {
							hdfsFileNmae = hdfsFile;
						}
						Path localPath = new Path(localFile);
						Path hdfsPath = new Path(hdfsFileNmae);
						// System.out.println("L --> H localFile " + localPath + " ---> hdfsPath " + hdfsPath);
						fs.copyFromLocalFile(deleteSourceFile, overwriteDestinationFile, localPath, hdfsPath);
						if (!hdfsFile.startsWith(File.separator)) {
							hdfsFullPath = fs.getWorkingDirectory().toString() + File.separator + hdfsFileNmae;
						} else {
							hdfsFullPath = fs.getUri().toString() + hdfsFileNmae;
						}
						message = "Successfully copied from " + localFile + "  to  " + hdfsFullPath + " .";
					}

					if (direction == copyDirection.copyToLocalFile) {
						if (localFile.endsWith(File.separator)) {
							hdfsFileNmae = hdfsFile.substring(hdfsFile.lastIndexOf(File.separator) + 1);
							localFileName = localFile + hdfsFileNmae;
						} else {
							localFileName = localFile;
						}
						Path localPath = new Path(localFileName);
						Path hdfsPath = new Path(hdfsFile);
						// System.out.println("H --> L hdfsPath " + hdfsPath + "  --->  localPath " + localPath);
						fs.copyToLocalFile(deleteSourceFile, hdfsPath, localPath, true);

						if (!hdfsFile.startsWith(File.separator)) {
							hdfsFullPath = fs.getWorkingDirectory().toString() + File.separator + hdfsFileNmae;
						} else {
							hdfsFullPath = fs.getUri().toString() + hdfsFile;
						}
						message = "Successfully copied from " + hdfsFullPath + "  to  " + localFileName + " .";
					}

				}
			}
		} catch (Exception e) {
			TRACE.log(TraceLevel.DEBUG, "Exception in copy process.", e);
			LOGGER.log(TraceLevel.ERROR, e.getMessage());
			message = "ERROR :" + e.getMessage();
		}
		long elapsedTime = Calendar.getInstance().getTimeInMillis() - millisStart;
		if (hasOutputPort) {
			submitOnOutputPort(message, elapsedTime);
		}

		// if operator is restarting in a consistent region, discard tuples
		if (isRestarting()) {
			if (TRACE.isLoggable(TraceLevel.DEBUG)) {
				TRACE.log(TraceLevel.DEBUG, "Restarting, discard: " + tuple.toString());
			}
			return;
		}

	}

	private void submitOnOutputPort(String message, long elapsedTime) throws Exception {

		if (TRACE.isLoggable(TraceLevel.DEBUG))
			TRACE.log(TraceLevel.DEBUG, "Submit filename and size on output port: " + message + " " + elapsedTime);

		OutputTuple outputTuple = outputPort.newTuple();

		outputTuple.setString(0, message);
		outputTuple.setLong(1, elapsedTime);

		// put the output tuple to the queue... to be submitted on process
		// thread
		if (crContext != null) {
			// if consistent region, queue and submit with permit
			outputPortQueue.put(outputTuple);
		} else if (outputPort != null) {
			// otherwise, submit immediately
			outputPort.submit(outputTuple);
		}
	}

	@Override
	public void shutdown() throws Exception {

		if (outputPortThread != null) {
			outputPortThread.interrupt();
		}

		super.shutdown();
	}

	@Override
	public void close() throws IOException {
		TRACE.log(TraceLevel.DEBUG, "StateHandler close", CONSISTEN_ASPECT);

	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		TRACE.log(TraceLevel.DEBUG, "Checkpoint " + checkpoint.getSequenceId(), CONSISTEN_ASPECT);

	}

	@Override
	public void drain() throws Exception {
		TRACE.log(TraceLevel.DEBUG, "Drain operator.", CONSISTEN_ASPECT);

		// tell file to flush all content from buffer

		// force any tuple to be submitted on the output port to flush
		if (outputPortQueue != null && outputPort != null) {
			while (outputPortQueue.peek() != null) {
				OutputTuple outputTuple = outputPortQueue.poll();
				if (outputTuple != null) {
					outputPort.submit(outputTuple);
				}
			}
		}
	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		TRACE.log(TraceLevel.DEBUG, "Reset to checkpoint " + checkpoint.getSequenceId(), CONSISTEN_ASPECT);

		unsetRestarting();
	}

	@Override
	public void resetToInitialState() throws Exception {
		TRACE.log(TraceLevel.DEBUG, "Reset to initial state", CONSISTEN_ASPECT);

		// create HDFS file with path from initial state
		// set as append mode to false, file should be overwritten
		// on reset to initial state

		unsetRestarting();
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		TRACE.log(TraceLevel.DEBUG, "Retire checkpoint", CONSISTEN_ASPECT);
	}

	private boolean isRestarting() {
		return isRestarting;
	}

	private void unsetRestarting() {
		TRACE.log(TraceLevel.DEBUG, "restarting set to false", CONSISTEN_ASPECT);
		isRestarting = false;
	}

	@Override
	protected void process() throws Exception {
		while (!shutdownRequested) {
			try {
				// System.out.println("process()!!!!!!.");
				OutputTuple tuple = outputPortQueue.take();
				if (outputPort != null) {

					if (TRACE.isLoggable(TraceLevel.DEBUG))
						TRACE.log(TraceLevel.DEBUG, "Submit output tuple: " + tuple.toString());

					// if operator is in consistent region, acquire permit
					// before submitting
					if (crContext != null) {
						crContext.acquirePermit();
					}
					outputPort.submit(tuple);
				}
			} catch (Exception e) {
				TRACE.log(TraceLevel.DEBUG, "Exception in output port thread.", e);

			} finally {
				// release permit when done submitting
				if (crContext != null) {
					crContext.releasePermit();
				}
			}
		}
	}
}

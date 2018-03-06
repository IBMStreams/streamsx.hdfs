/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.streams.operator.Attribute;
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
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;

@SharedLoader
public class HDFS2FileSink extends AbstractHdfsOperator implements StateHandler {

	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.HDFS2FileSink"; 
	private static final String CONSISTEN_ASPECT = "com.ibm.streamsx.hdfs.HDFS2FileSink.consistent"; 

	/**
	 * Create a logger specific to this class
	 */
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME); 

	private static Logger TRACE = Logger.getLogger(CLASS_NAME);

	// do not set as null as it can cause complication for checkpoing
	// use empty string
	private String rawFileName = ""; 
	private String file = null;
	private String tempFile = ""; 
	private String timeFormat = "yyyyMMdd_HHmmss"; 
	private String currentFileName;
	private String currentTempFileName = ""; 


	private HdfsFile fFileToWrite;

	private long bytesPerFile = -1;
	private long tuplesPerFile = -1;
	private double timePerFile = -1;
	private boolean closeOnPunct = false;
	private String encoding = null;
	private String fileAttrName = null;
	// this will be reset if the file index is 0.
	private int dataIndex = 0;
	private int fileIndex = -1;
	private boolean dynamicFilename;
	private MetaType dataType = null;

	// file num for generating FILENUM variable in filename
	private int fileNum = 0;

	// Variables required by the optional output port
	// hasOutputPort signifies if the operator has output port defined or not
	// assuming in the beginning that the operator does not have a error output
	// port by setting hasOutputPort to false

	private boolean hasOutputPort = false;
	private StreamingOutput<OutputTuple> outputPort;
	
	private LinkedBlockingQueue<OutputTuple> outputPortQueue;
	private Thread outputPortThread;

	private Thread fFileTimerThread;
	private InitialState initState;
	private boolean isRestarting;
	private ConsistentRegionContext crContext;

	
	
	private class InitialState {
		String path; 
		String rawFilename; 
		
		private InitialState() {
			
			if (fFileToWrite != null)
			{
				path = fFileToWrite.getPath();
			}
			
			rawFileName = HDFS2FileSink.this.rawFileName;
		}
	}	

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
						Messages.getString("HDFS_SINK_OUTPUT_PORT"), 
						null);

			} else {
				if (streamingOutputPort.getStreamSchema().getAttribute(0)
						.getType().getMetaType() != Type.MetaType.RSTRING) {
					checker.setInvalidContext(
							Messages.getString("HDFS_SINK_FIRST_OUTPUT_PORT"), 
							null);
				}
				if (streamingOutputPort.getStreamSchema().getAttribute(1)
						.getType().getMetaType() != Type.MetaType.UINT64) {
					checker.setInvalidContext(
							Messages.getString("HDFS_SINK_SECOND_OUTPUT_PORT"), 
							null);

				}

			}

		}
	}

	@Parameter(name = IHdfsConstants.PARAM_FILE_NAME_ATTR, optional = true, description = "The name of the attribute containing the filename.")
	public void setFilenameAttr(String name) {
		fileAttrName = name;
	}

	@Parameter(optional = true)
	public void setFile(String file) {
		TRACE.log(TraceLevel.DEBUG, "setFile: " + file); 
		this.file = file;
	}

	public String getFile() {
		return file;
	}

	@Parameter(optional = true)
	public void setTempFile(String tempFile) {
		TRACE.log(TraceLevel.DEBUG, "setTempFile: " + tempFile); 
		this.tempFile = tempFile;
	}

	public String getTempFile() {
		return tempFile;
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

	/**
	 * This function checks only things that can be determined at compile time.
	 * 
	 * @param checker
	 * @throws Exception
	 */
	@ContextCheck(compile = true)
	public static void checkInputPortSchema(OperatorContextChecker checker)
			throws Exception {
		// rstring or ustring would need to be provided.
		StreamSchema inputSchema = checker.getOperatorContext()
				.getStreamingInputs().get(0).getStreamSchema();
		boolean hasDynamic = checker.getOperatorContext().getParameterNames()
				.contains(IHdfsConstants.PARAM_FILE_NAME_ATTR);
		if (!hasDynamic && inputSchema.getAttributeCount() != 1) {
			checker.setInvalidContext(
					Messages.getString("HDFS_SINK_ONE_ATTR_INPUT_PORT", IHdfsConstants.PARAM_FILE_NAME_ATTR), new Object[] {} ); 
		}

		if (hasDynamic && inputSchema.getAttributeCount() != 2) {
			checker.setInvalidContext(
					Messages.getString("HDFS_SINK_TWO_ATTR_INPUT_PORT", IHdfsConstants.PARAM_FILE_NAME_ATTR, IHdfsConstants.PARAM_FILE_NAME_ATTR ) , new Object[] {});
		}

		if (inputSchema.getAttributeCount() == 1) {
			// check that the attribute type must be a rstring or ustring
			if (MetaType.RSTRING != inputSchema.getAttribute(0).getType()
					.getMetaType()
					&& MetaType.USTRING != inputSchema.getAttribute(0)
							.getType().getMetaType()
					&& MetaType.BLOB != inputSchema.getAttribute(0).getType()
							.getMetaType()) {
				checker.setInvalidContext(
						Messages.getString("HDFS_SINK_INVALID_ATTR_TYPE", inputSchema.getAttribute(0).getType().getMetaType()), null);
			}
		}
		if (inputSchema.getAttributeCount() == 2) {
			int numString = 0;
			int numBlob = 0;
			for (int i = 0; i < 2; i++) {
				MetaType t = inputSchema.getAttribute(i).getType()
						.getMetaType();
				if (MetaType.USTRING == t || MetaType.RSTRING == t) {
					numString++;
				} else if (MetaType.BLOB == t) {
					numString++;
				}
			} // end for loop;

			if (numBlob == 0 && numString == 2 || // data is a string
					numBlob == 1 && numString == 1) { // data is a blob
				// we're golden.
			} else {
				checker.setInvalidContext(
						Messages.getString("HDFS_SINK_INVALID_ATTR_FILENAME_DATA"), 
						null);
			}
		}
	}

	@ContextCheck(compile = true)
	public static void checkCompileParameters(OperatorContextChecker checker)
			throws Exception {
		checker.checkExcludedParameters("file", 
				IHdfsConstants.PARAM_FILE_NAME_ATTR);
		checker.checkExcludedParameters(IHdfsConstants.PARAM_FILE_NAME_ATTR,
				"file"); 
		checker.checkExcludedParameters(IHdfsConstants.PARAM_BYTES_PER_FILE,
				IHdfsConstants.PARAM_TIME_PER_FILE,
				IHdfsConstants.PARAM_TUPLES_PER_FILE,
				IHdfsConstants.PARAM_FILE_NAME_ATTR);
		checker.checkExcludedParameters(IHdfsConstants.PARAM_TIME_PER_FILE,
				IHdfsConstants.PARAM_BYTES_PER_FILE,
				IHdfsConstants.PARAM_TUPLES_PER_FILE,
				IHdfsConstants.PARAM_FILE_NAME_ATTR);
		checker.checkExcludedParameters(IHdfsConstants.PARAM_TUPLES_PER_FILE,
				IHdfsConstants.PARAM_BYTES_PER_FILE,
				IHdfsConstants.PARAM_TIME_PER_FILE,
				IHdfsConstants.PARAM_FILE_NAME_ATTR);
		checker.checkExcludedParameters(IHdfsConstants.PARAM_FILE_NAME_ATTR,
				IHdfsConstants.PARAM_TUPLES_PER_FILE,
				IHdfsConstants.PARAM_BYTES_PER_FILE,
				IHdfsConstants.PARAM_TIME_PER_FILE);

	}
	
	@ContextCheck(compile = true)
	public static void checkConsistentRegion(OperatorContextChecker checker) {
		
		// check that the file sink is not at the start of the consistent region
		OperatorContext opContext = checker.getOperatorContext();
		ConsistentRegionContext crContext = opContext.getOptionalContext(ConsistentRegionContext.class);
		if (crContext != null) {
			if (crContext.isStartOfRegion()) {
				checker.setInvalidContext(Messages.getString("HDFS_NOT_CONSISTENT_REGION", "HDFS2FileSink"), null); 
			}
			// check that tempFile parameter is not used in consistent region
			Set<String> parameters = opContext.getParameterNames();
			if (parameters.contains("tempFile")) { 
				checker.setInvalidContext(Messages.getString("HDFS_SINK_INVALID_PARAM_TEMPFILE", "HDFS2FileSink"), null); 
			}
		}
	}

	@ContextCheck(compile = false)
	public static void checkParameters(OperatorContextChecker checker)
			throws Exception {
		List<String> paramValues = checker.getOperatorContext()
				.getParameterValues("file"); 
		List<String> tempFileValues = checker.getOperatorContext()
				.getParameterValues("tempFile"); 
		List<String> timeFormatValue = checker.getOperatorContext()
				.getParameterValues("timeFormat"); 
		if (timeFormatValue != null) {
			if (!timeFormatValue.isEmpty()) {
				if (timeFormatValue.get(0).isEmpty()) {
					throw new Exception("Operator parameter timeFormat should not be empty.");
				}
			}
		}
		for (String fileValue : paramValues) {
			if (fileValue.contains(IHdfsConstants.FILE_VAR_PREFIX)) {
				if (!fileValue.contains(IHdfsConstants.FILE_VAR_HOST)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_PROCID)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_PEID)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_PELAUNCHNUM)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_TIME)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_FILENUM)) {
					throw new Exception(
							"Unsupported % specification provided. Supported values are %HOST, %PEID, %FILENUM, %PROCID, %PELAUNCHNUM, %TIME");
				}
			}
		}
		for (String fileValue : tempFileValues) {
			if (fileValue.contains(IHdfsConstants.FILE_VAR_PREFIX)) {
				if (!fileValue.contains(IHdfsConstants.FILE_VAR_HOST)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_PROCID)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_PEID)
						&& !fileValue.contains(IHdfsConstants.FILE_VAR_PELAUNCHNUM)
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
			if (Long.valueOf(bytesPerFileVal.get(0)) < 0) {
				checker.setInvalidContext(
						Messages.getString("HDFS_SINK_INVALID_VALUE_BYTEPERFILE"), 
						null);
			}
		}

		if (!tuplesPerFileVal.isEmpty()) {
			if (Long.valueOf(tuplesPerFileVal.get(0)) < 0) {
				checker.setInvalidContext(
						Messages.getString("HDFS_SINK_INVALID_VALUE_TUPLESPERFILE"), 
						null);
			}
		}

		if (!timeForFileVal.isEmpty()) {
			if (Float.valueOf(timeForFileVal.get(0)) < 0.0) {
				checker.setInvalidContext(
						Messages.getString("HDFS_SINK_INVALID_VALUE_TIMEPERFIL"), 
						null);
			}
		}

		int dataAttribute = 0;
		int fileAttribute = -1;
		StreamSchema inputSchema = checker.getOperatorContext()
				.getStreamingInputs().get(0).getStreamSchema();
		if (checker.getOperatorContext().getParameterNames()
				.contains(IHdfsConstants.PARAM_FILE_NAME_ATTR)) {
			String fileNameAttr = checker.getOperatorContext()
					.getParameterValues(IHdfsConstants.PARAM_FILE_NAME_ATTR)
					.get(0);
			fileAttribute = inputSchema.getAttribute(fileNameAttr).getIndex();
			if (fileAttribute == 0) {
				// default data attribute of 0 is not right, so need to fix
				// that.
				dataAttribute = 1;
			}
		}
		// now, check the data attribute is an okay type.
		MetaType dataType = inputSchema.getAttribute(dataAttribute).getType()
				.getMetaType();
		// check that the data type is okay.
		if (dataType != MetaType.RSTRING && dataType != MetaType.USTRING
				&& dataType != MetaType.BLOB) {
			checker.setInvalidContext(
					Messages.getString("HDFS_SINK_INVALID_DATA_ATTR_TYPE", dataType),  
							null);
		}
		if (fileAttribute != -1) {
			// If we have a filename attribute, let's check that it's the right
			// type.
			if (MetaType.RSTRING != inputSchema.getAttribute(1).getType()
					.getMetaType()
					&& MetaType.USTRING != inputSchema.getAttribute(1)
							.getType().getMetaType()) {
				checker.setInvalidContext(
						Messages.getString("HDFS_SINK_INVALID_ATTR_FILENAME", inputSchema.getAttribute(1).getType().getMetaType()), 
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
		StreamSchema inputSchema = checker.getOperatorContext()
				.getStreamingInputs().get(0).getStreamSchema();
		List<String> fileAttrNameList = checker.getOperatorContext()
				.getParameterValues(IHdfsConstants.PARAM_FILE_NAME_ATTR);
		if (fileAttrNameList == null || fileAttrNameList.size() == 0) {
			// Nothing to check, because the parameter doesn't exist.
			return;
		}

		String fileAttrName = fileAttrNameList.get(0);
		Attribute fileAttr = inputSchema.getAttribute(fileAttrName);
		if (fileAttr == null) {
			checker.setInvalidContext(Messages.getString("HDFS_SINK_NO_ATTRIBUTE"), 
					new Object[] { fileAttrName });
		}
		if (MetaType.RSTRING != fileAttr.getType().getMetaType()
				&& MetaType.USTRING != fileAttr.getType().getMetaType()) {
			checker.setInvalidContext(
					Messages.getString("HDFS_SINK_INVALID_ATTR_FILENAME", fileAttr.getType().getMetaType()), 
					new Object[] {});
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
							Messages.getString("HDFS_SINK_INVALID_SCHEMA", fileUri.getScheme(), hdfsUri.getScheme()),
							 null); 
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
							Messages.getString("HDFS_SINK_INVALID_HOST", fileUri.getAuthority(), hdfsUri.getAuthority()),
							 null); 
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
			
			crContext = context.getOptionalContext(ConsistentRegionContext.class);

		
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
		
		// register for data governance
		// only register if static filename mode
		TRACE.log(TraceLevel.INFO, "HDFS2FileSink - Data Governance - file: " + file + " and HdfsUri: " + getHdfsUri());  
		if (fileAttrName == null && file != null && getHdfsUri() != null) {
			registerForDataGovernance(getHdfsUri(), file);
		}
		
		/*
		 * Set appropriate variables if the optional output port is
		 * specified. Also set outputPort to the output port at index 0
		 */
		if (context.getNumberOfStreamingOutputs() == 1) {

			hasOutputPort = true;

			outputPort = context.getStreamingOutputs().get(0);
			
			// create a queue and thread for submitting tuple on the output port
			// this is done to allow us to properly support consistent region
			// where we must acquire consistent region permit before doin submission.
			// And allow us to submit tuples when a reset happens.
			outputPortQueue = new LinkedBlockingQueue<>();			
			outputPortThread = createProcessThread();
			outputPortThread.start();

		}
		
		if (fileAttrName != null) {
			// We are in dynamic filename mode.
			dynamicFilename = true;

			// We have already verified that we aren't using file in a context
			// check.
			// We have also already verified that the input schema has two
			// attributes.

			// We have also verified that it's in the input scheme and that it's
			// type is okay.
			// What we need to do here is get its index.
			StreamSchema inputSchema = context.getStreamingInputs().get(0)
					.getStreamSchema();
			Attribute fileAttr = inputSchema.getAttribute(fileAttrName);
			fileIndex = fileAttr.getIndex();
			if (fileIndex == 1)
				dataIndex = 0;
			else if (fileIndex == 0) {
				dataIndex = 1;
			} else {
				throw new Exception(
						"Attribute "
								+ fileAttrName
								+ " must be either attribute 0 or 1 on the input stream.");
			}
		}
		StreamSchema inputSchema = context.getStreamingInputs().get(0)
				.getStreamSchema();
		// Save the data type for later use.
		dataType = inputSchema.getAttribute(dataIndex).getType().getMetaType();
		if (!dynamicFilename) {
			Date date = Calendar.getInstance().getTime();
			currentFileName = refreshCurrentFileName(file, date, false);
			if (tempFile.isEmpty()) {
				createFile(currentFileName);
			} else {
				currentTempFileName	= refreshCurrentFileName(tempFile, date, true);
				createFile(currentTempFileName);
			}
		}
		
		initRestarting(context);
		
		// take a snapshot of initial state
		initState = new InitialState();
	}

	private void registerForDataGovernance(String serverURL, String file) {
		TRACE.log(TraceLevel.INFO, "HDFS2FileSink - Registering for data governance with server URL: " + serverURL + " and file: " + file);						  
		
		Map<String, String> properties = new HashMap<String, String>();
		properties.put(IGovernanceConstants.TAG_REGISTER_TYPE, IGovernanceConstants.TAG_REGISTER_TYPE_OUTPUT);
		properties.put(IGovernanceConstants.PROPERTY_OUTPUT_OPERATOR_TYPE, "HDFS2FileSink"); 
		properties.put(IGovernanceConstants.PROPERTY_SRC_NAME, file);
		properties.put(IGovernanceConstants.PROPERTY_SRC_TYPE, IGovernanceConstants.ASSET_HDFS_FILE_TYPE);
		properties.put(IGovernanceConstants.PROPERTY_SRC_PARENT_PREFIX, "p1"); 
		properties.put("p1" + IGovernanceConstants.PROPERTY_SRC_NAME, serverURL); 
		properties.put("p1" + IGovernanceConstants.PROPERTY_SRC_TYPE, IGovernanceConstants.ASSET_HDFS_SERVER_TYPE); 
		properties.put("p1" + IGovernanceConstants.PROPERTY_PARENT_TYPE, IGovernanceConstants.ASSET_HDFS_SERVER_TYPE_SHORT); 
		TRACE.log(TraceLevel.INFO, "HDFS2FileSink - Data governance: " + properties.toString()); 
		
		setTagData(IGovernanceConstants.TAG_OPERATOR_IGC, properties);				
	}
	
	private void createFile(String filename) {
		
		if (TRACE.isLoggable(TraceLevel.DEBUG)) {
			TRACE.log(TraceLevel.DEBUG,	"Create File: " + filename); 
		}
		
		fFileToWrite = new HdfsFile(getOperatorContext(), filename,
				getHdfsClient(), getEncoding(), dataIndex, dataType);
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

	private String refreshCurrentFileName(String baseName, Date date, boolean isTempFile)
			throws UnknownHostException {

		// We must preserve the file parameter in order for us
		// to support multi-file in the operator

		// Check if % specification mentioned are valid or not
		String currentFileName = baseName;
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
			SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
			currentFileName = currentFileName.replace(
					IHdfsConstants.FILE_VAR_TIME, sdf.format(date));
			int anumber = fileNum;
			if (isTempFile) anumber--; //temp files get the number of the last generated file name
			currentFileName = currentFileName.replace(
					IHdfsConstants.FILE_VAR_FILENUM, String.valueOf(anumber));
			if ( ! isTempFile ) { //only the final file names increment 
				fileNum++;
			}
		}
		return currentFileName;
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
		else if (isCloseOnPunct()) {

			// This handles the closeOnPunct expiration policy
			TRACE.log(TraceLevel.DEBUG, "Close on punct, close file."); 

			closeFile();

		}

	}

	private void closeFile() throws Exception {

		TRACE.log(TraceLevel.DEBUG, "closeFile()"); 

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

		// If Optional output port is present output the filename and file
		// size

		synchronized (this) {
			
			if (fFileToWrite != null) {
				boolean alreadyClosed = fFileToWrite.isClosed();
	
				fFileToWrite.setExpired();
				fFileToWrite.close();
	
				if ( ! alreadyClosed) {
					String target = fFileToWrite.getPath();
					boolean success = true;
					if ( ! tempFile.isEmpty() ) {
						target = currentFileName;
						if (getHdfsClient().exists(target)) {
							if (getHdfsClient().delete(target, false)) {
								TRACE.log(TraceLevel.DEBUG, "Successfully removed file: " + target); 
							} else {
								TRACE.log(TraceLevel.ERROR, "Failed to removed file: " + target); 
							}
						}
						if (getHdfsClient().rename(currentTempFileName, target)) {
							TRACE.log(TraceLevel.DEBUG, "Successfully renamed file: " + currentTempFileName +" to: " + target);  
						} else {
							success = false;
							TRACE.log(TraceLevel.ERROR, "Failed to rename file: " + currentTempFileName +" to: " + target);  
						}
					}
					// operators can perform additional 
					if (hasOutputPort && success) {
						submitOnOutputPort(target, fFileToWrite.getSizeFromHdfs());
					}
				}
			}
		}

	}

	@Override
	synchronized public void process(StreamingInput<Tuple> stream, Tuple tuple)
			throws Exception {

		// if operator is restarting in a consistent region, discard tuples
		if (isRestarting())
		{
			if (TRACE.isLoggable(TraceLevel.DEBUG)) {
				TRACE.log(TraceLevel.DEBUG,	"Restarting, discard: " + tuple.toString()); 
			}
			return;
		}
		
		if (dynamicFilename) {
			String filenameString = tuple.getString(fileIndex);
			if (rawFileName == null || rawFileName.isEmpty()) {
				// the first tuple. No raw file name is set.
				rawFileName = filenameString;
				Date date = Calendar.getInstance().getTime();
				currentFileName = refreshCurrentFileName(rawFileName, date, false);
				String realName = currentFileName;
				if ( ! tempFile.isEmpty()) {
					currentTempFileName	= refreshCurrentFileName(tempFile, date, true);
					realName = currentTempFileName;
				}
				createFile(realName);
				if (TRACE.isLoggable(Level.INFO))
					TRACE.info("Created first file " + currentFileName 
							+ " from raw " + rawFileName + " real fileName " + realName);  
			}

			if (!rawFileName.equals(filenameString)) {
				// the filename has changed. Notice this cannot happen on the
				// first tuple.
				closeFile();
				rawFileName = filenameString;
				Date date = Calendar.getInstance().getTime();
				currentFileName = refreshCurrentFileName(rawFileName, date, false);
				String realName = currentFileName;
				if ( ! tempFile.isEmpty()) {
					currentTempFileName	= refreshCurrentFileName(tempFile, date, true);
					realName = currentTempFileName;
				}
				createFile(realName);
				if (TRACE.isLoggable(Level.INFO))
					TRACE.info("Updating filename -- new name is " 
							+ currentFileName + " from raw " + rawFileName+ " real fileName " + realName);  
			}
			// When we leave this block, we know the file is ready to be written
			// to.
		}

		if (fFileToWrite != null) {

			if (fFileToWrite.isExpired()) {
				// these calls will set fFileToWrite to the new file
				Date date = Calendar.getInstance().getTime();
				currentFileName = refreshCurrentFileName(file, date, false);
				String realName = currentFileName;
				if ( ! tempFile.isEmpty()) {
					currentTempFileName	= refreshCurrentFileName(tempFile, date, true);

					realName = currentTempFileName;
				}
				createFile(realName);
			}

			fFileToWrite.writeTuple(tuple);
			// This will check bytesPerFile and tuplesPerFile expiration policy
			if (fFileToWrite.isExpired()) {
				closeFile();
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

		// put the output tuple to the queue... to be submitted on process thread
		if (crContext != null)
		{
			// if consistent region, queue and submit with permit
			outputPortQueue.put(outputTuple);
		}
		else if (outputPort != null){
			// otherwise, submit immediately
			outputPort.submit(outputTuple);
		}
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
		
		if (outputPortThread != null) {
			outputPortThread.interrupt();
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

	@Override
	public void close() throws IOException {
		TRACE.log(TraceLevel.DEBUG, "StateHandler close", CONSISTEN_ASPECT); 

	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		TRACE.log(TraceLevel.DEBUG, "Checkpoint " + checkpoint.getSequenceId(), 
				CONSISTEN_ASPECT);	
		
		// get file size, tuple count and filename
		String path = fFileToWrite.getPath();
		long tupleCnt = fFileToWrite.getTupleCnt();
		long size = fFileToWrite.getSize();
		
		
		checkpoint.getOutputStream().writeObject(path);
		checkpoint.getOutputStream().writeLong(tupleCnt);		
		checkpoint.getOutputStream().writeLong(size);
		checkpoint.getOutputStream().writeInt(fileNum);
		
		if (dynamicFilename)
		{
			if (rawFileName==null)
				rawFileName = ""; 
			checkpoint.getOutputStream().writeObject(rawFileName);
		}

	}

	@Override
	public void drain() throws Exception {
		TRACE.log(TraceLevel.DEBUG, "Drain operator.", CONSISTEN_ASPECT);		 
		
		// tell file to flush all content from buffer
		fFileToWrite.flush();
		
		// force any tuple to be submitted on the output port to flush
		if (outputPortQueue != null && outputPort != null)
		{
			while (outputPortQueue.peek() != null)
			{
				OutputTuple outputTuple = outputPortQueue.poll();
				if (outputTuple != null)
				{
					outputPort.submit(outputTuple);
				}
			}
		}
	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		TRACE.log(TraceLevel.DEBUG,
				"Reset to checkpoint " + checkpoint.getSequenceId(), 
				CONSISTEN_ASPECT);	

		// close current file
		if (fFileToWrite != null)
			closeFile();
				
		String path = (String)checkpoint.getInputStream().readObject();
		long tupleCnt = checkpoint.getInputStream().readLong();
		long size =  checkpoint.getInputStream().readLong();
		int fileNum = checkpoint.getInputStream().readInt();
		
		if (dynamicFilename)
		{
			rawFileName = (String) checkpoint.getInputStream().readObject();
			
			if (rawFileName == null)
				rawFileName = ""; 
		}
		
		// create HDFS file with path from checkpoint
		// set as append mode to not overwrite content of file
		// on reset		
		createFile(path);
		fFileToWrite.setTupleCnt(tupleCnt);
		fFileToWrite.setSize(size);
		fFileToWrite.setAppend(true);		
		
		currentFileName = path;
		this.fileNum = fileNum;
		
		unsetRestarting();
	}

	@Override
	public void resetToInitialState() throws Exception {
		TRACE.log(TraceLevel.DEBUG, "Reset to initial state", CONSISTEN_ASPECT); 

		// close current file
		if (fFileToWrite != null)
			closeFile();
				
		String path = initState.path;
		fileNum = 0;
		
		if (dynamicFilename)
		{
			rawFileName = initState.rawFilename;
			
			if (rawFileName == null)
				rawFileName = ""; 
		}
		
		// create HDFS file with path from initial state
		// set as append mode to false, file should be overwritten
		// on reset to initial state
	
		// path can be null in dynamic filename mode
		// as the name comes in from incoming tuples
		if (path != null) {
			createFile(path);
			fFileToWrite.setTupleCnt(0);
			fFileToWrite.setSize(0);
			fFileToWrite.setAppend(false);
			
			// increment to 1 as we have created a file
			currentFileName = path;
			fileNum = 1;
		}
		
		unsetRestarting();
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		TRACE.log(TraceLevel.DEBUG, "Retire checkpoint", CONSISTEN_ASPECT); 
	}
	
	private boolean isRestarting()
	{
		return isRestarting;
	}
	
	private void initRestarting(OperatorContext opContext)
	{
		TRACE.log(TraceLevel.DEBUG, "restarting set to true", CONSISTEN_ASPECT); 
		isRestarting = false;
		if (crContext != null )
		{
			int relaunchCount = opContext.getPE().getRelaunchCount();
			if (relaunchCount > 0)
			{
				System.out.println(Messages.getString("HDFS_SINK_SET_RESTARTING")); 
				isRestarting = true;
			}
		}		
	}
	
	private void unsetRestarting()
	{
		TRACE.log(TraceLevel.DEBUG, "restarting set to false", CONSISTEN_ASPECT); 
		isRestarting = false;
	}

	
	@Override
	protected void process() throws Exception {		
		while (!shutdownRequested)
		{			
			try {
				//System.out.println("process()!!!!!!.");
				OutputTuple tuple = outputPortQueue.take();
				if (outputPort != null)
				{
					
					if (TRACE.isLoggable(TraceLevel.DEBUG))
						TRACE.log(TraceLevel.DEBUG, "Submit output tuple: " + tuple.toString()); 
					
					// if operator is in consistent region, acquire permit before submitting
					if (crContext != null)
					{
						crContext.acquirePermit();
					}					
					outputPort.submit(tuple);
				}
			} catch (Exception e) {
				TRACE.log(TraceLevel.DEBUG,
						"Exception in output port thread.", e); 

			} finally {			
				// release permit when done submitting
				if (crContext != null)
				{
					crContext.releasePermit();
				}			
			}			
		}
	}
}

/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.fs.FileStatus;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.SharedLoader;

@SharedLoader
public class HDFS2DirectoryScan extends AbstractHdfsOperator {

	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.HDFSDirectoryScan";

	// should use logger not tied to LOG_FACILITY to send to trace file instead of log file
	// TODO - Error / Warning messages in the LOG need to be put in the
	// messages.properties
	// file
	private static Logger TRACE = Logger.getLogger(CLASS_NAME);

	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME, "com.ibm.streamsx.hdfs.BigDataMessages");

	private final String NUM_SCANS_METRIC = "nScans";
	private Metric nScans;

	long initDelayMil = 0;

	private long sleepTimeMil = 5000;

	private String pattern;
	private String directory = "";
	private boolean isStrictMode;
	private double initDelay;
	private double sleepTime = 5;

	private Object dirLock = new Object();
	private boolean finalPunct = false;

	@Parameter(optional = true)
	public void setDirectory(String directory) {
		TRACE.entering(CLASS_NAME, "setDirectory", directory);
		this.directory = directory;
	}

	public String getDirectory() {
		return directory;
	}

	@Parameter(optional = true)
	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public String getPattern() {
		return pattern;
	}

	@Parameter(optional = true)
	public void setInitDelay(double initDelay) {
		this.initDelay = initDelay;
	}

	@Parameter(optional = true)
	public void setSleepTime(double sleepTime) {
		this.sleepTime = sleepTime;
	}

	@Parameter(optional = true)
	public void setStrictMode(boolean strictMode) {
		this.isStrictMode = strictMode;
	}

	public boolean isStrictMode() {
		return isStrictMode;
	}

	@ContextCheck()
	public static void checkInputPortSchema(OperatorContextChecker checker) {
		List<StreamingInput<Tuple>> streamingInputs = checker.getOperatorContext().getStreamingInputs();
		if (streamingInputs.size() > 0) {
			StreamSchema inputSchema = streamingInputs.get(0).getStreamSchema();
			if (inputSchema.getAttributeCount() > 1) {
				checker.setInvalidContext("The control input port of HDFSDirectoryScan expects a single attribute. The type of this attribute has to be rstring.", null);
			}

			if (inputSchema.getAttribute(0).getType()
					.getMetaType() != MetaType.RSTRING) {
				checker.setInvalidContext(
						"Expected attribute of type rstring, found attribute of type "
								+ inputSchema.getAttribute(0).getType()
										.getMetaType(), null);
			}
		}
	}
	
	@ContextCheck(compile = true)
	public static void checkOutputPortSchema(OperatorContextChecker checker)
			throws Exception {
		StreamSchema outputSchema = checker.getOperatorContext().getStreamingOutputs().get(0).getStreamSchema();
		
		if(outputSchema.getAttributeCount()!=1){
			checker.setInvalidContext("The output port of HDFSDirectoryScan operator expects a single attribute. The type of this attribute has to be rstring.", null);
		}
		if (outputSchema.getAttribute(0).getType()
				.getMetaType() != MetaType.RSTRING) {
			checker.setInvalidContext(
					"Expected attribute of type rstring, found attribute of type "
							+ outputSchema.getAttribute(0).getType()
									.getMetaType(), null);
		}
	}

	@ContextCheck()
	public static void checkParameter(OperatorContextChecker checker) {
		int numInputPorts = checker.getOperatorContext().getNumberOfStreamingInputs();
		if (numInputPorts == 0) {
			Set<String> paramNames = checker.getOperatorContext().getParameterNames();
			if (!paramNames.contains("directory")) {
				checker.setInvalidContext(
						"The directory parameter must be specified if the operator does not have any input port.", null);
			}
		}
	}
	
	@ContextCheck(compile = false)
	public static void checkRunTimeError(OperatorContextChecker checker) {
		if (!checker.getOperatorContext().getParameterValues(IHdfsConstants.PARAM_SLEEP_TIME).isEmpty()) {
			if (Integer.valueOf(checker.getOperatorContext()
					.getParameterValues(IHdfsConstants.PARAM_SLEEP_TIME).get(0)) < 0) {
				checker.setInvalidContext(
						"Operator parameter sleepTime value should not be less than 0.",
						null);
			}
		}
		
		if (!checker.getOperatorContext().getParameterValues(IHdfsConstants.PARAM_INITDELAY).isEmpty()) {
			if (Integer.valueOf(checker.getOperatorContext()
					.getParameterValues(IHdfsConstants.PARAM_INITDELAY).get(0)) < 0) {
				checker.setInvalidContext(
						"Operator parameter initDelay value should not be less than 0.",
						null);
			}
		}
		
		/*Check if the pattern is valid.
		 * Set invalid context otherwise.
		 */
		if (checker.getOperatorContext().getParameterNames().contains("pattern")){
			String pattern = checker.getOperatorContext().getParameterValues("pattern").get(0);
			try{
				java.util.regex.Pattern.compile(pattern);	
			}
			catch(PatternSyntaxException e){
				checker.setInvalidContext(pattern+" is a invalid pattern",null);
			}
		}
	}

	@ContextCheck(compile=false)
	public static void checkUriMatch(OperatorContextChecker checker) throws Exception {
		List<String> hdfsUriParamValues = checker.getOperatorContext().getParameterValues("hdfsUri");
		List<String> dirParamValues = checker.getOperatorContext().getParameterValues("directory");
		
		String hdfsUriValue = null;
		if(hdfsUriParamValues.size() == 1)
			hdfsUriValue = hdfsUriParamValues.get(0);
		
		String dirValue = null;
		if(dirParamValues.size() == 1)
			dirValue = dirParamValues.get(0);

		
		// only need to perform this check if both 'hdfsUri' and 'file' params are set
		if(hdfsUriValue != null && dirValue != null) {
			URI hdfsUri;
			URI dirUri;
			try {
				hdfsUri = new URI(hdfsUriValue);
			} catch (URISyntaxException e) {
				TRACE.log(TraceLevel.ERROR, "'hdfsUri' parameter contains an invalid URI: " + hdfsUriValue);
				throw e;
			}
			
			try {
				dirUri = new URI(dirValue);
			} catch (URISyntaxException e) {
				TRACE.log(TraceLevel.ERROR, "'dirValue' parameter contains an invalid URI: " + dirValue);
				throw e;
			}
			
			if(dirUri.getScheme() != null) {
				// must have the same scheme
				if(!hdfsUri.getScheme().equals(dirUri.getScheme())) {
					checker.setInvalidContext("The 'directory' scheme (" + dirUri.getScheme() + ") must match the 'hdfsUri' scheme (" + hdfsUri.getScheme() + ")", null);
					return;
				}
				
				// must have the same authority
				if((hdfsUri.getAuthority() == null && dirUri.getAuthority() != null) ||
				   (hdfsUri.getAuthority() != null && dirUri.getAuthority() == null) ||
				   (hdfsUri.getAuthority() != null && dirUri.getAuthority() != null 
				   	&& !hdfsUri.getAuthority().equals(dirUri.getAuthority()))) {
					checker.setInvalidContext("The host and port specified by the 'directory' parameter (" + dirUri.getAuthority() + ") must match the host and port specified by the 'hdfsUri' parameter (" + hdfsUri.getAuthority() + ")", null);
					return;
				}	
			}
		}		
	}
	
	public void checkStrictMode(OperatorContext context) throws Exception{
		boolean checked = false;
		// directory can be empty
		
		//When a directory parameter is not specified, check if control input port
		//is present. Warn if so, else throw an exception
		if (!context.getParameterNames().contains("directory"))
		{
			// if strict mode, directory can be empty if we have an input stream
			if (context.getNumberOfStreamingInputs() == 0) {
				throw new Exception("directory parameter needs to be specified when control input port is not present.");
			} else {
				// warn user that this may be a problem.
				LOGGER.log(LogLevel.WARN,"directory parameter is not specified. Scanning based on input from control port.");
				checked = true;
			}
		}
		if (isStrictMode) {
			if(!checked){
				if(directory.isEmpty()){
					throw new Exception("strictMode parameter is set to true but directory parameter is empty");
				}
				else if (!getHdfsClient().exists(directory)) {
					throw new Exception("strictMode parameter is set to true but directory "+directory+" does not exist.");
				}
				else if (!getHdfsClient().isDirectory(directory))
				{
					throw new Exception("directory parameter value "+directory+" does not refer to a valid directory");
				}
			}
		} else {
			if(!checked){
				if (directory.isEmpty()) {
					if(context.getNumberOfStreamingInputs()==1){
						LOGGER.log(LogLevel.WARN, "Directory parameter value is empty");
						directory="";
					}
					else{
						throw new Exception("Directory parameter value is empty and control input port is not specified");
					}
				}
				else if (!getHdfsClient().exists(directory)) {
					//TRACE.warning("Directory specified does not exist: " + directory);
					LOGGER.log(LogLevel.WARN, "Directory specified does not exist: " + directory);
				}
				else if (!getHdfsClient().isDirectory(directory))
				{
					if(context.getNumberOfStreamingInputs()==1){
						//throw new Exception("directory parameter value "+directory+" does not refer to a valid directory");
						LOGGER.log(LogLevel.WARN, "Directory parameter value "+directory+" does not refer to a valid directory");
						directory="";// so that it does not break in process
					}
					else{
						throw new Exception("Directory parameter value "+directory+" does not refer to a valid directory");
					}
				}
				else{
					try{
						scanDirectory(directory);
					}
					catch(IOException ex){
						if(context.getNumberOfStreamingInputs()==1){
							LOGGER.log(LogLevel.WARN,ex.getMessage());
							directory="";
						}
						else{
							throw ex;
						}
					}
				}
			}
		}
	}

	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		if(directory != null) {
			try {
				URI uri = new URI(directory);
				TRACE.log(TraceLevel.DEBUG, "uri: " + uri.toString());
				
				String scheme = uri.getScheme();
				if(scheme != null) {
					String fs;
					if(uri.getAuthority() != null)
						fs = scheme + "://" + uri.getAuthority();
					else
						fs = scheme + ":///";
					
					if(getHdfsUri() == null)
						setHdfsUri(fs);
					
					TRACE.log(TraceLevel.DEBUG, "fileSystemUri: " + getHdfsUri());
					
					String path = directory.substring(fs.length());
					
					if(!path.startsWith("/"))
						path = "/" + path;
					
					setDirectory(path);
				}
			} catch(URISyntaxException e) {
				TRACE.log(TraceLevel.DEBUG, "Unable to construct URI: " + e.getMessage());
				
				throw e;
			}	
		}
		
		super.initialize(context);
		
		// Associate the aspect Log with messages from the SPL log
		// logger.
		// setLoggerAspects(LOGGER.getName(), "HDFSDirectoryScan");

		// Converstion of the Operator Parameters from seconds to MilliSeconds
		sleepTimeMil = (long) (1000 * sleepTime);
		initDelayMil = (long) (1000 * initDelay);

		// TODO - Need to change following for annotation based metrics
		nScans = context.getMetrics().getCustomMetric(NUM_SCANS_METRIC);

		checkStrictMode(context);
		
		processThread = createProcessThread();

	}

	@Override
	public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
		String newDir = tuple.getString(0);
		boolean dirExists = true;

		if (TRACE.isLoggable(TraceLevel.INFO))
			TRACE.log(TraceLevel.INFO, "Control signal received: " + newDir);

		if (newDir != null) {
			synchronized (dirLock) {

				TRACE.log(TraceLevel.DEBUG, "Acquired dirLock for control signal");

				if (isStrictMode) {
					if (newDir != null && !getHdfsClient().exists(newDir)) {
						dirExists = false;
						throw new Exception("Directory specified from control input port does not exist: " + newDir);
					}
					else if (newDir != null && !getHdfsClient().isDirectory(newDir)) {
						dirExists = false;
						throw new Exception("Directory specified from control input port is not a valid directory: " + newDir);
					}
					else if(newDir!=null && newDir.isEmpty()){
						dirExists = false;
						throw new Exception("Directory received from input port is empty.");
					}
				} else {
					
					if (newDir != null && newDir.isEmpty()) {
						dirExists = false;
						// if directory is empty and number of input port is zero, throw
						// exception
						// warn user that this may be a problem.
						//if (TRACE.isLoggable(TraceLevel.WARNING))
							//TRACE.warning("directory received from input port is empty.");
						LOGGER.log(LogLevel.WARN, "Directory received from input port is empty.");
					}					
					else if (newDir != null && !getHdfsClient().exists(newDir)) {
						dirExists = false;
						//if (TRACE.isLoggable(TraceLevel.WARNING))
							//TRACE.warning("Directory specified from control input port does not exist: " + newDir);
						LOGGER.log(LogLevel.WARN,"Directory specified from control input port does not exist: "+newDir);
					}
					else if (newDir != null && !getHdfsClient().isDirectory(newDir)) {
						dirExists = false;
						//if (TRACE.isLoggable(TraceLevel.WARNING))
							//TRACE.warning("Directory specified from control input port is not a valid directory: " + newDir);
						LOGGER.log(LogLevel.WARN, "Directory specified from control input port is not a valid directory: " + newDir);
					}
					else if (newDir!=null){
						try{
							scanDirectory(newDir);
						}
						catch(IOException e){
							dirExists = false;
							LOGGER.log(LogLevel.WARN,e.getMessage());
						}
					}
				}

				if (newDir != null && !newDir.isEmpty() && !directory.equals(newDir) && dirExists) {
					setDirectory(newDir);
				}
				// always notify to allow user to send a signal
				// to force a scan immediately.
				dirLock.notify();
			}
		}
	}
	
	@Override
	public void processPunctuation(StreamingInput<Tuple> arg0, Punctuation arg1) throws Exception {
		// if final marker
		if (arg1 == Punctuation.FINAL_MARKER)
		{
			if (TRACE.isLoggable(TraceLevel.DEBUG))
				TRACE.log(TraceLevel.DEBUG, "Received final punctuation");
			// wake up the process thread
			// cause the process loop to terminate so we do not keep scanning
			synchronized(dirLock) {
				finalPunct = true;
				dirLock.notifyAll();
			}
		}
	}

	protected void process() throws IOException {

		if (initDelayMil > 0) {
			try {
				Thread.sleep(initDelayMil);
			} catch (InterruptedException e) {
				TRACE.info("Initial delay interrupted");
			}
		}

		long lastTimestamp = 0;
		while (!shutdownRequested && !finalPunct) {

			long scanStartTime = System.currentTimeMillis();

			FileStatus[] files = new FileStatus[0];
			String currentDir = "";
			
			synchronized (dirLock) {
				if (TRACE.isLoggable(TraceLevel.DEBUG))
					TRACE.log(TraceLevel.DEBUG, "Acquired dirLock for scanDirectory");
				
				currentDir = getDirectory();
				// only scan if a directory is specified
				
				if (!currentDir.isEmpty()) {
					files = scanDirectory(currentDir);
				}
			}

			if (files.length > 0) {

				long lastTimestampInThisScan = 0;
				for (FileStatus file : files) {
					if (file.isDirectory()) {
						if (TRACE.isLoggable(LogLevel.INFO)) {
							TRACE.log(TraceLevel.INFO, "Skipping " + file.toString() + " because it is a directory.");
						}
					}

					else {
						long fileTimestamp = file.getModificationTime();
						if (TRACE.isLoggable(TraceLevel.DEBUG))
							TRACE.log(TraceLevel.DEBUG, "File: " + file.getPath().toString() + " " + fileTimestamp);
						if (fileTimestamp > lastTimestamp) {

							lastTimestampInThisScan = Math.max(fileTimestamp, lastTimestampInThisScan);
							// file path can be retrieved from URI without
							// parsing
							String filePath = file.getPath().toUri().getPath();

							if (filePath != null) {
								OutputTuple outputTuple = getOutput(0).newTuple();
								if (TRACE.isLoggable(TraceLevel.INFO))
									TRACE.log(TraceLevel.INFO, "Submit File: " + file.getPath().toString());
								outputTuple.setString(0, filePath);
								try {
									getOutput(0).submit(outputTuple);
								} catch (Exception e) {
									LOGGER.log(TraceLevel.ERROR, "Problem submitting tuple " + e);
								}
							}
						}
					}
				}
				lastTimestamp = Math.max(lastTimestamp, lastTimestampInThisScan);
			}

			// TODO: What happens if the scan takes so long
			// to finish, and we start again immediately?
			synchronized (dirLock) {
				if (TRACE.isLoggable(TraceLevel.DEBUG))
					TRACE.log(TraceLevel.DEBUG, "Acquire dir lock to detect changes in process method");

				// if no control signal has come in, wait...				
				
				if (getDirectory().equals(currentDir)) {

					if (TRACE.isLoggable(TraceLevel.DEBUG))
						TRACE.log(TraceLevel.DEBUG, "Directory not changed, check if we need to sleep.");

					long currentTime = System.currentTimeMillis();
					long timeBeforeNextScan = sleepTimeMil - (currentTime - scanStartTime);
					if (timeBeforeNextScan > 0) {
						try {
							if (TRACE.isLoggable(TraceLevel.INFO))
								TRACE.log(TraceLevel.INFO, "Sleeping for..." + timeBeforeNextScan);
							dirLock.wait(timeBeforeNextScan);
						} catch (Exception e) {
							TRACE.log(TraceLevel.DEBUG, "Sleep time interrupted");

						} finally {
							if (!getDirectory().equals(currentDir)) {
								TRACE.log(TraceLevel.DEBUG, "Directory changed, reset lastTimestamp");
								lastTimestamp = 0;
							}
						}
					}
				} else {
					TRACE.log(TraceLevel.DEBUG, "Directory changed, reset lastTimestamp");
					lastTimestamp = 0;
				}
			}
		}
		
		if (TRACE.isLoggable(TraceLevel.DEBUG))
			TRACE.log(TraceLevel.DEBUG, "Exited directory scan loop");
		
		try {
			getOutput(0).punctuate(Punctuation.FINAL_MARKER);
		} catch (Exception e) {
			LOGGER.log(TraceLevel.ERROR, "Problem sending final punctuation " + e);
		}
	}

	private FileStatus[] scanDirectory(String directory) throws IOException {

		if (TRACE.isLoggable(TraceLevel.INFO))
			TRACE.log(TraceLevel.INFO, "scanDirectory: " + directory);

		FileStatus[] files = new FileStatus[0];
		nScans.incrementValue(1);
		files = getHdfsClient().scanDirectory(directory, getPattern());
		return files;
	}
	
	@Override
	public void shutdown() throws Exception {
		 if (processThread != null) {
	            processThread.interrupt();
	            processThread = null;
	        }
	        OperatorContext context = getOperatorContext();
	        TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );	        

	        // Must call super.shutdown()
	        super.shutdown();
	}

}
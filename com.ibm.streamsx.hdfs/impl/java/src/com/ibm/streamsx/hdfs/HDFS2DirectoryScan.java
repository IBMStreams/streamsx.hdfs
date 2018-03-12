/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
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
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;

@SharedLoader
public class HDFS2DirectoryScan extends AbstractHdfsOperator implements StateHandler {

	private static final String CLASS_NAME = "com.ibm.streamsx.hdfs.HDFSDirectoryScan"; 

	private static final Object CONSISTEN_ASPECT = HDFS2DirectoryScan.class.getName() + ".consistent"; 

	// should use logger not tied to LOG_FACILITY to send to trace file instead
	// of log file
	// TODO - Error / Warning messages in the LOG need to be put in the
	// messages.properties
	// file
	private static Logger TRACE = Logger.getLogger(CLASS_NAME);

	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME); 
 
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

	private ConsistentRegionContext crContext;

	private long fLastTimestamp;
	private String fInitialDir;

	// timestamp and filename of last submitted file
	private long fLastSubmittedTs;
	private String fLastSubmittedFilename;
	
	// timestamp and filename to reset to.  Only initialized if reset is called
	private long fResetToTs = -1;
	private String fResetToFilename = ""; 

	private class ModTimeComparator implements Comparator {

		@Override
		public int compare(Object arg0, Object arg1) {
			if (arg0 instanceof FileStatus && arg1 instanceof FileStatus) {
				FileStatus s1 = (FileStatus) arg0;
				FileStatus s2 = (FileStatus) arg1;

				long diff = s1.getModificationTime() - s2.getModificationTime();
				if (diff < 0) {
					return -1;
				} else if (diff > 0) {
					return 1;
				}
			}
			return 0;
		}
	}

	private class FileNameComparator implements Comparator {

		@Override
		public int compare(Object arg0, Object arg1) {
			if (arg0 instanceof FileStatus && arg1 instanceof FileStatus) {
				FileStatus s1 = (FileStatus) arg0;
				FileStatus s2 = (FileStatus) arg1;

				return s1.getPath().compareTo(s2.getPath());
			}
			return 0;
		}
	}

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
	
	@ContextCheck(compile = true)
	public static void checkConsistentRegion(OperatorContextChecker checker) {
				
		OperatorContext opContext = checker.getOperatorContext();
		ConsistentRegionContext crContext = opContext.getOptionalContext(ConsistentRegionContext.class);
		if (crContext != null)
		{
			if (crContext.isStartOfRegion() && opContext.getNumberOfStreamingInputs()>0)
			{
				checker.setInvalidContext(Messages.getString("HDFS_NOT_CONSISTENT_REGION", "HDFS2DirectoryScan"), null); 
			}
		}
	}

	@ContextCheck()
	public static void checkInputPortSchema(OperatorContextChecker checker) {
		List<StreamingInput<Tuple>> streamingInputs = checker.getOperatorContext().getStreamingInputs();
		if (streamingInputs.size() > 0) {
			StreamSchema inputSchema = streamingInputs.get(0).getStreamSchema();
			if (inputSchema.getAttributeCount() > 1) {
				checker.setInvalidContext(
						Messages.getString("HDFS_DS_INVALID_INPUT_PORT"), 
						null);
			}

			if (inputSchema.getAttribute(0).getType().getMetaType() != MetaType.RSTRING) {
				checker.setInvalidContext(Messages.getString("HDFS_DS_INVALID_ATTRIBUTE", 
						 inputSchema.getAttribute(0).getType().getMetaType()), null);
			}

			ConsistentRegionContext crContext = checker.getOperatorContext().getOptionalContext(
					ConsistentRegionContext.class);
			if (crContext != null) {
				LOGGER.log( LogLevel.WARNING, Messages.getString("HDFS_DS_CONSISTENT_REGION_NOT_SUPPORTED")); 
			}
		}

	}

	@ContextCheck(compile = true)
	public static void checkOutputPortSchema(OperatorContextChecker checker) throws Exception {
		StreamSchema outputSchema = checker.getOperatorContext().getStreamingOutputs().get(0).getStreamSchema();

		if (outputSchema.getAttributeCount() != 1) {
			checker.setInvalidContext(
					Messages.getString("HDFS_DS_INVALID_OUTPUT_PORT"), 
					null);
		}
		if (outputSchema.getAttribute(0).getType().getMetaType() != MetaType.RSTRING) {
			checker.setInvalidContext(Messages.getString("HDFS_DS_INVALID_ATTRIBUTE") 
					+ outputSchema.getAttribute(0).getType().getMetaType(), null);
		}
	}

	@ContextCheck()
	public static void checkParameter(OperatorContextChecker checker) {
		int numInputPorts = checker.getOperatorContext().getNumberOfStreamingInputs();
		if (numInputPorts == 0) {
			Set<String> paramNames = checker.getOperatorContext().getParameterNames();
			if (!paramNames.contains("directory")) { 
				checker.setInvalidContext(
						Messages.getString("HDFS_DS_INVALID_DIRECTORY_PARAM"), null); 
			}
		}
	}

	@ContextCheck(compile = false)
	public static void checkRunTimeError(OperatorContextChecker checker) {
		if (!checker.getOperatorContext().getParameterValues(IHdfsConstants.PARAM_SLEEP_TIME).isEmpty()) {
			if (Integer
					.valueOf(checker.getOperatorContext().getParameterValues(IHdfsConstants.PARAM_SLEEP_TIME).get(0)) < 0) {
				checker.setInvalidContext(Messages.getString("HDFS_DS_INVALID_SLEEP_TIMER_PARAM"), null); 
			}
		}

		if (!checker.getOperatorContext().getParameterValues(IHdfsConstants.PARAM_INITDELAY).isEmpty()) {
			if (Integer.valueOf(checker.getOperatorContext().getParameterValues(IHdfsConstants.PARAM_INITDELAY).get(0)) < 0) {
				checker.setInvalidContext(Messages.getString("HDFS_DS_INVALID_INIT_DELAY_PARAM"), null); 
			}
		}

		/*
		 * Check if the pattern is valid. Set invalid context otherwise.
		 */
		if (checker.getOperatorContext().getParameterNames().contains("pattern")) { 
			String pattern = checker.getOperatorContext().getParameterValues("pattern").get(0); 
			try {
				java.util.regex.Pattern.compile(pattern);
			} catch (PatternSyntaxException e) {
				checker.setInvalidContext(pattern + Messages.getString("HDFS_DS_INVALID_PATTERN_PARAM"), null); 
			}
		}
	}

	@ContextCheck(compile = false)
	public static void checkUriMatch(OperatorContextChecker checker) throws Exception {
		List<String> hdfsUriParamValues = checker.getOperatorContext().getParameterValues("hdfsUri"); 
		List<String> dirParamValues = checker.getOperatorContext().getParameterValues("directory"); 

		String hdfsUriValue = null;
		if (hdfsUriParamValues.size() == 1)
			hdfsUriValue = hdfsUriParamValues.get(0);

		String dirValue = null;
		if (dirParamValues.size() == 1)
			dirValue = dirParamValues.get(0);

		// only need to perform this check if both 'hdfsUri' and 'file' params
		// are set
		if (hdfsUriValue != null && dirValue != null) {
			URI hdfsUri;
			URI dirUri;
			try {
				hdfsUri = new URI(hdfsUriValue);
			} catch (URISyntaxException e) {
				LOGGER.log(TraceLevel.ERROR, Messages.getString("HDFS_DS_INVALID_URL_PARAM", "hdfsUri", hdfsUriValue)); 
				throw e;
			}

			try {
				dirUri = new URI(dirValue);
			} catch (URISyntaxException e) {
				LOGGER.log(TraceLevel.ERROR, Messages.getString("HDFS_DS_INVALID_URL_PARAM", "dirValue", dirValue)); 
				throw e;
			}

			if (dirUri.getScheme() != null) {
				// must have the same scheme
				if (!hdfsUri.getScheme().equals(dirUri.getScheme())) {
					checker.setInvalidContext(Messages.getString("HDFS_DS_INVALID_DIRECTORY_SCHEMA" ,dirUri.getScheme() , hdfsUri.getScheme()), null); 
					return;
				}

				// must have the same authority
				if ((hdfsUri.getAuthority() == null && dirUri.getAuthority() != null)
						|| (hdfsUri.getAuthority() != null && dirUri.getAuthority() == null)
						|| (hdfsUri.getAuthority() != null && dirUri.getAuthority() != null && !hdfsUri.getAuthority()
								.equals(dirUri.getAuthority()))) {
					checker.setInvalidContext(
							Messages.getString("HDFS_DS_INVALID_HOST_DIRECTORY_SCHEMA", dirUri.getAuthority() , hdfsUri.getAuthority()) , null);
					return;
				}
			}
		}
	}

	public void checkStrictMode(OperatorContext context) throws Exception {
		boolean checked = false;
		// directory can be empty

		// When a directory parameter is not specified, check if control input
		// port
		// is present. Warn if so, else throw an exception
		if (!context.getParameterNames().contains("directory")) { 
			// if strict mode, directory can be empty if we have an input stream
			if (context.getNumberOfStreamingInputs() == 0) {
				throw new Exception("directory parameter needs to be specified when control input port is not present.");
			} else {
				// warn user that this may be a problem.
				LOGGER.log(LogLevel.WARN,
						Messages.getString("HDFS_DS_NOT_SPECIFIED_DIR_PARAM")); 
				checked = true;
			}
		}
		if (isStrictMode) {
			if (!checked) {
				if (directory.isEmpty()) {
					throw new Exception(Messages.getString("HDFS_DS_EMPTY_DIRECTORY_STRICT_MODE")); 
				} else if (!getHdfsClient().exists(directory)) {
					throw new Exception(Messages.getString("HDFS_DS_DIRECTORY_NOT_EXIST_STRICT_MODE", directory));
				} else if (!getHdfsClient().isDirectory(directory)) {
					throw new Exception(Messages.getString("HDFS_DS_IS_NOT_A_DIRECTORY", directory));
				}
			}
		} else {
			if (!checked) {
				if (directory.isEmpty()) {
					if (context.getNumberOfStreamingInputs() == 1) {
						LOGGER.log(LogLevel.WARN, Messages.getString("HDFS_DS_EMPTY_DIRECTORY_PARAM")); 
						directory = ""; 
					} else {
						throw new Exception(Messages.getString("HDFS_DS_EMPTY_DIRECTORY_NOT_CONTROL_PORT")); 
					}
				} else if (!getHdfsClient().exists(directory)) {
					// TRACE.warning("Directory specified does not exist: " +
					// directory);
					LOGGER.log(LogLevel.WARN, Messages.getString("HDFS_DS_DIRECTORY_NOT_EXIST", directory)); 
				} else if (!getHdfsClient().isDirectory(directory)) {
					if (context.getNumberOfStreamingInputs() == 1) {
						// throw new
						// Exception("directory parameter value "+directory+" does not refer to a valid directory");
						LOGGER.log(LogLevel.WARN, Messages.getString("HDFS_DS_IS_NOT_A_DIRECTORY", directory)); 
						directory = "";// so that it does not break in process 
					} else {
						throw new Exception(Messages.getString("HDFS_DS_IS_NOT_A_DIRECTORY", directory));
					}
				} else {
					try {
						scanDirectory(directory);
					} catch (IOException ex) {
						if (context.getNumberOfStreamingInputs() == 1) {
							LOGGER.log(LogLevel.WARN, ex.getMessage());
							directory = ""; 
						} else {
							throw ex;
						}
					}
				}
			}
		}
	}

	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		if (directory != null) {
			try {
				URI uri = new URI(directory);
				TRACE.log(TraceLevel.DEBUG, "uri: " + uri.toString()); 

				String scheme = uri.getScheme();
				if (scheme != null) {
					String fs;
					if (uri.getAuthority() != null)
						fs = scheme + "://" + uri.getAuthority(); 
					else
						fs = scheme + ":///"; 

					if (getHdfsUri() == null)
						setHdfsUri(fs);

					TRACE.log(TraceLevel.DEBUG, "fileSystemUri: " + getHdfsUri()); 

					String path = directory.substring(fs.length());

					if (!path.startsWith("/")) 
						path = "/" + path; 

					setDirectory(path);
				}
			} catch (URISyntaxException e) {
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

		crContext = context.getOptionalContext(ConsistentRegionContext.class);
		fInitialDir = getDirectory();
		processThread = createProcessThread();
	}

	@Override
	public void allPortsReady() throws Exception {
		super.allPortsReady();

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
					} else if (newDir != null && !getHdfsClient().isDirectory(newDir)) {
						dirExists = false;
						throw new Exception("Directory specified from control input port is not a valid directory: "
								+ newDir);
					} else if (newDir != null && newDir.isEmpty()) {
						dirExists = false;
						throw new Exception("Directory received from input port is empty.");
					}
				} else {

					if (newDir != null && newDir.isEmpty()) {
						dirExists = false;
						// if directory is empty and number of input port is
						// zero, throw exception
						// warn user that this may be a problem.
						LOGGER.log(LogLevel.WARN, Messages.getString("HDFS_DS_EMPTY_DIRECTORY_INPUT_PORT")); 
					} else if (newDir != null && !getHdfsClient().exists(newDir)) {
						dirExists = false;
						LOGGER.log(LogLevel.WARN, Messages.getString("HDFS_DS_DIRECTORY_NOT_EXIST_INPUT_PORT", newDir));
									} else if (newDir != null && !getHdfsClient().isDirectory(newDir)) {
						dirExists = false;
						LOGGER.log(LogLevel.WARN,
								Messages.getString("HDFS_DS_INVALID_DIRECTORY_INPUT_PORT", newDir)); 
					} else if (newDir != null) {
						try {
							scanDirectory(newDir);
						} catch (IOException e) {
							dirExists = false;
							LOGGER.log(LogLevel.WARN, e.getMessage());
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
		if (arg1 == Punctuation.FINAL_MARKER) {
			if (TRACE.isLoggable(TraceLevel.DEBUG))
				TRACE.log(TraceLevel.DEBUG, "Received final punctuation"); 
			// wake up the process thread
			// cause the process loop to terminate so we do not keep scanning
			synchronized (dirLock) {
				finalPunct = true;
				dirLock.notifyAll();
			}
		}
	}

	private void scanDirOnce() throws Exception {
		// unit of work for scanning the directory and submitting tuple.

		String currentDir = null;

		// do not allow reset to happen, while we are trying to figure out the
		// timestamp
		// also disallow changing directory once this has started
		long latestTimeFromLastCycle = 0;
		String latestFilenameFromLastCycle = ""; 
		try {
			if (crContext != null) {
				crContext.acquirePermit();
			}
			synchronized (dirLock) {
				currentDir = getDirectory();
			}
			
			if (fResetToTs != -1)
			{
				latestTimeFromLastCycle = fResetToTs;
				latestFilenameFromLastCycle = fResetToFilename;
				
				// unset these variables as we are about
				// to actually scan the directory again
				fResetToTs = -1;
				fResetToFilename = ""; 
			}
			else {
				latestTimeFromLastCycle = fLastSubmittedTs;
				latestFilenameFromLastCycle = fLastSubmittedFilename;
			}
		
			
			if (TRACE.isLoggable(TraceLevel.DEBUG))
				debug("latestTimeFromLastCycle: " + latestTimeFromLastCycle, null); 

		} finally {
			if (crContext != null) {
				crContext.releasePermit();
			}
		}

		if (currentDir != null) {
			FileStatus[] files = scanDirectory(currentDir);

			// this returns a list of files, sorted by modification time.
			for (int i = 0; i < files.length; i++) {

				FileStatus currentFile = files[i];
				
				// if reset, get out of loop immediately
				// next scan will use the reset timestamp and filename
				if (fResetToTs != -1)
				{
					// Set the last submitted ts and filename to the reset value
					// but reset does not happen until the next scan is done.
					// These two variables represent the last fully processed file.
					// If a checkpoint is done before the next can be completed,
					// these two variables will be checkpointed, as they represent
					// the last consistent state.
					fLastSubmittedTs = fResetToTs;
					fLastSubmittedFilename = fResetToFilename;
					break;
				}

				if (TRACE.isLoggable(TraceLevel.DEBUG))
					debug("Found File: " + currentFile.getPath().toString() + " " + currentFile.getModificationTime(), CONSISTEN_ASPECT);  

				List<FileStatus> currentSet = new ArrayList<FileStatus>();
				currentSet.add(currentFile);

				// look at the next file, if the next file has the same
				// timestamp... collect all
				// files with same timestamp
				if (i + 1 < files.length) {
					FileStatus nextFile = files[i + 1];
					if (nextFile.getModificationTime() == currentFile.getModificationTime()) {
						// this returns a list of files with same timestamp, in
						// alphabetical order
						List<FileStatus> filesWithSameTs = collectFilesWithSameTimestamp(files, i);
						i += filesWithSameTs.size() - 1;
						currentSet = filesWithSameTs;
					}
				}

				for (Iterator iterator = currentSet.iterator(); iterator.hasNext();) {
					FileStatus fileToSubmit = (FileStatus) iterator.next();
					
					// if reset, get out of loop immediately
					// next scan will use the reset timestamp and filename
					if (fResetToTs != -1)
						break;

					String filePath = fileToSubmit.getPath().toUri().getPath();

					// if file is newer, always submit
					if (!fileToSubmit.isDirectory() && filePath != null
							&& fileToSubmit.getModificationTime() > latestTimeFromLastCycle) {
						OutputTuple outputTuple = getOutput(0).newTuple();
						if (TRACE.isLoggable(TraceLevel.DEBUG))
							debug("Submit File: " + fileToSubmit.getPath().toString() + " "  
									+ fileToSubmit.getModificationTime(),CONSISTEN_ASPECT);
						outputTuple.setString(0, filePath);

						try {
							if (crContext != null) {
								crContext.acquirePermit();
							}
							getOutput(0).submit(outputTuple);
							fLastSubmittedTs = fileToSubmit.getModificationTime();
							fLastSubmittedFilename = fileToSubmit.getPath().toUri().getPath();

							if (crContext.isTriggerOperator())
								crContext.makeConsistent();
						} finally {
							if (crContext != null) {
								crContext.releasePermit();
							}
						}
					} else if (!fileToSubmit.isDirectory() && filePath != null
							&& fileToSubmit.getModificationTime() == latestTimeFromLastCycle && currentSet.size() > 1) {

						// if file has same timestamp, then a file should only
						// be submitted if
						// the filename is > than the last filename

						if (filePath.compareTo(latestFilenameFromLastCycle) > 0)
						{						
							OutputTuple outputTuple = getOutput(0).newTuple();
							if (TRACE.isLoggable(TraceLevel.DEBUG))
								debug("Submit File: " + fileToSubmit.getPath().toString() + " "  
										+ fileToSubmit.getModificationTime(), CONSISTEN_ASPECT);
							outputTuple.setString(0, filePath);
	
							try {
								if (crContext != null) {
									crContext.acquirePermit();
								}
								getOutput(0).submit(outputTuple);
								fLastSubmittedTs = fileToSubmit.getModificationTime();
								fLastSubmittedFilename = fileToSubmit.getPath().toUri().getPath();
	
								crContext.makeConsistent();
							} finally {
								if (crContext != null) {
									crContext.releasePermit();
								}
							}
						}						
					}

				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private List<FileStatus> collectFilesWithSameTimestamp(FileStatus[] files, int index) {

		ArrayList<FileStatus> toReturn = new ArrayList<FileStatus>();
		FileStatus currentFile = files[index];
		toReturn.add(currentFile);

		if (TRACE.isLoggable(TraceLevel.INFO))
			TRACE.log(TraceLevel.INFO, "Collect files with same timestamp: " + currentFile.getPath() + ":"  
					+ currentFile.getModificationTime());

		for (int j = index + 1; j < files.length; j++) {
			FileStatus nextFile = files[j];
			if (nextFile.getModificationTime() == currentFile.getModificationTime()) {
				toReturn.add(nextFile);
				if (TRACE.isLoggable(TraceLevel.INFO))
					TRACE.log(TraceLevel.INFO, "Collect files with same timestamp: " + nextFile.getPath() + ":"  
							+ nextFile.getModificationTime());
			} else {
				break;
			}
		}

		Collections.sort(toReturn, new FileNameComparator());

		return toReturn;
	}

	protected void process() throws IOException {
		if (crContext != null) {
			processConsistent();
		} else {
			processNotConsistent();
		}
	}

	protected void processConsistent() throws IOException {
		if (initDelayMil > 0) {
			try {
				Thread.sleep(initDelayMil);
			} catch (InterruptedException e) {
				TRACE.info("Initial delay interrupted"); 
			}
		}

		while (!shutdownRequested && !finalPunct) {
			try {
				scanDirOnce();
				synchronized (dirLock) {
					dirLock.wait(sleepTimeMil);
				}
			} catch (Exception e) {
			}
		}

		if (TRACE.isLoggable(TraceLevel.DEBUG))
			TRACE.log(TraceLevel.DEBUG, "Exited directory scan loop"); 

		try {
			getOutput(0).punctuate(Punctuation.FINAL_MARKER);
		} catch (Exception e) {
			LOGGER.log(TraceLevel.ERROR, Messages.getString("HDFS_DS_PUNCTUATION_FAILED") + e); 
		}
	}

	protected void processNotConsistent() throws IOException {

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
					} else {
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
									LOGGER.log(TraceLevel.ERROR, Messages.getString("HDFS_DS_SUBMITTING_FAILED", e));
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
			LOGGER.log(TraceLevel.ERROR, Messages.getString("HDFS_DS_PUNCTUATION_FAILED", e)); 
		}

	}

	private FileStatus[] scanDirectory(String directory) throws IOException {

		if (TRACE.isLoggable(TraceLevel.INFO))
			TRACE.log(TraceLevel.INFO, "scanDirectory: " + directory); 

		FileStatus[] files = new FileStatus[0];

		nScans.incrementValue(1);
		files = getHdfsClient().scanDirectory(directory, getPattern());

		Arrays.sort(files, new ModTimeComparator());
		return files;
	}

	@Override
	public void shutdown() throws Exception {
		if (processThread != null) {
			processThread.interrupt();
			processThread = null;
		}
		OperatorContext context = getOperatorContext();
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " shutting down in PE: "  
				+ context.getPE().getPEId() + " in Job: " + context.getPE().getJobId()); 

		// Must call super.shutdown()
		super.shutdown();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		debug("Checkpoint " + checkpoint.getSequenceId(), CONSISTEN_ASPECT); 

		// checkpoint scan time and directory
		checkpoint.getOutputStream().writeObject(getDirectory());

		// when checkpoint, save the timestamp - 1 to get the tuples to replay
		// as we are always looking for file that is larger that the last
		// timestamp
		checkpoint.getOutputStream().writeLong(fLastSubmittedTs);		
		debug( "Checkpoint timestamp " + fLastSubmittedTs, CONSISTEN_ASPECT); 
		
		checkpoint.getOutputStream().writeObject(fLastSubmittedFilename);
		debug("Checkpoint filename " + fLastSubmittedFilename, CONSISTEN_ASPECT); 

	}

	@Override
	public void drain() throws Exception {
		debug("Drain", CONSISTEN_ASPECT); 

	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		debug("Reset to checkpoint " + checkpoint.getSequenceId(), CONSISTEN_ASPECT); 

		String ckptDir = (String) checkpoint.getInputStream().readObject();

		synchronized (dirLock) {
			setDirectory(ckptDir);
		}

		fResetToTs = checkpoint.getInputStream().readLong();
		debug("Reset timestamp " + fResetToTs, CONSISTEN_ASPECT); 
		
		fResetToFilename = (String)checkpoint.getInputStream().readObject();
		debug("Reset filename " + fResetToFilename, CONSISTEN_ASPECT); 
	}

	@Override
	public void resetToInitialState() throws Exception {
		debug("Reset to initial state", CONSISTEN_ASPECT); 

		synchronized (dirLock) {
			setDirectory(fInitialDir);
		}
		fResetToTs = 0;
		fResetToFilename = ""; 

	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		debug("Retire checkpoint " + id, CONSISTEN_ASPECT);
	}
	
	private void debug(String message, Object aspect)
	{
		TRACE.log(TraceLevel.DEBUG, message, aspect);
	}

}
/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2013, 2014                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
package com.ibm.streamsx.hdfs;

public interface IHdfsConstants {

	public static final String PARAM_TIME_PER_FILE = "timePerFile";
	public static final String PARAM_CLOSE_ON_PUNCT = "closeOnPunct";
	public static final String PARAM_TUPLES_PER_FILE = "tuplesPerFile";
	public static final String PARAM_BYTES_PER_FILE = "bytesPerFile";
	public static final String PARAM_SLEEP_TIME = "sleepTime";
	public static final String PARAM_INITDELAY = "initDelay";
	public static final String PARAM_ENCODING = "encoding";
	public static final String FILE_VAR_PREFIX = "%";
	public static final String FILE_VAR_FILENUM = "%FILENUM";
	public static final String FILE_VAR_TIME = "%TIME";
	public static final String FILE_VAR_PELAUNCHNUM = "%PELAUNCHNUM";
	public static final String FILE_VAR_PEID = "%PEID";
	public static final String FILE_VAR_PROCID = "%PROCID";
	public static final String FILE_VAR_HOST = "%HOST";

	public final static String AUTH_PRINCIPAL = "authPrincipal";
	public final static String AUTH_KEYTAB = "authKeytab";
	public final static String CRED_FILE = "credFile";
	
	public final static String FS_HDFS = "hdfs";
	public final static String FS_GPFS = "gpfs";
	public final static String FS_WEBHDFS = "webhdfs";
	
	
}

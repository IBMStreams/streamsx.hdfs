package com.ibm.streamsx.hdfs;

public interface IGovernanceConstants {
	public static final String TAG_OPERATOR_IGC = "OperatorIGC";
	public static final String TAG_REGISTER_TYPE = "registerType";
	public static final String TAG_REGISTER_TYPE_INPUT = "input";
	public static final String TAG_REGISTER_TYPE_OUTPUT = "output";
	
	
	public static final String ASSET_HDFS_FILE_TYPE = "$Streams-HDFSFile";
	public static final String ASSET_HDFS_SERVER_TYPE = "$Streams-HDFSServer";
	public static final String ASSET_HDFS_SERVER_TYPE_SHORT = "$HDFSServer";
		
	public static final String PROPERTY_SRC_NAME = "srcName";
	public static final String PROPERTY_SRC_TYPE = "srcType";
	
//	public static final String PROPERTY_SRC_PARENT_PREFIX = "srcParentPrefix";
	public static final String PROPERTY_SRC_PARENT_PREFIX = "srcParent";
	public static final String PROPERTY_PARENT_TYPE = "parentType";
	
	public static final String PROPERTY_PARENT_PREFIX = "p1";
	public static final String PROPERTY_GRANDPARENT_PREFIX = "p2";
	
	public static final String PROPERTY_INPUT_OPERATOR_TYPE = "inputOperatorType";
	public static final String PROPERTY_OUTPUT_OPERATOR_TYPE = "outputOperatorType";
	
}

/*******************************************************************************
 * Licensed Materials - Property of IBM
 * Copyright IBM Corp. 2014
 * US Government Users Restricted Rights - Use, duplication or
 * disclosure restricted by GSA ADP Schedule Contract with
 * IBM Corp.
 *******************************************************************************/

package com.ibm.streamsx.hdfs.client;

public class UnsupportedSchemeException extends Exception {
	
	private static final long serialVersionUID = 1L;
	private static final String MESSAGE = "Scheme is not supported: ";
	
	public UnsupportedSchemeException(String scheme) {
		super(MESSAGE + scheme);
	}
}

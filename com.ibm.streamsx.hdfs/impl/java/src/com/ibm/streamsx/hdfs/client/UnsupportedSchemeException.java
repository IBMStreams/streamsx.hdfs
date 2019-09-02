/*******************************************************************************
 * Copyright (C) 2014-2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs.client;

public class UnsupportedSchemeException extends Exception {

	private static final long serialVersionUID = 1L;
	private static final String MESSAGE = "Scheme is not supported: ";

	public UnsupportedSchemeException(String scheme) {
		super(MESSAGE + scheme);
	}
}

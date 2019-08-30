
package com.ibm.streamsx.hdfs.client;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexExcludePathFilter implements PathFilter {
	/* begin_generated_IBM_copyright_code */
	public static final String IBM_COPYRIGHT = " Licensed Materials-Property of IBM                              "
			+ " 5724-Y95                                                        "
			+ " (C) Copyright IBM Corp.  2013, 2019    All Rights Reserved.     "
			+ " US Government Users Restricted Rights - Use, duplication or     "
			+ " disclosure restricted by GSA ADP Schedule Contract with         "
			+ " IBM Corp.                                                       "
			+ "                                                                 ";
	/* end_generated_IBM_copyright_code */

	private final String regex;

	public RegexExcludePathFilter(String regex) {
		this.regex = regex;
	}

	@Override
	public boolean accept(Path path) {
		return path.getName().matches(regex);
	}

}

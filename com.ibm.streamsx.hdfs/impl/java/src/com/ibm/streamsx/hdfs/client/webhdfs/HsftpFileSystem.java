/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.streamsx.hdfs.client.webhdfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;


/**
 * An implementation of a protocol for accessing filesystems over HTTPS. The
 * following implementation provides a limited, read-only interface to a
 * filesystem over HTTPS.
 *
 * @see org.apache.hadoop.hdfs.server.namenode.ListPathsServlet
 * @see org.apache.hadoop.hdfs.server.namenode.FileDataServlet
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HsftpFileSystem extends HftpFileSystem {
	public static final Text TOKEN_KIND = new Text("HSFTP delegation");
	public static final String SCHEME = "hsftp";

	/**
	 * Return the protocol scheme for the FileSystem.
	 * <p/>
	 *
	 * @return <code>hsftp</code>
	 */
	@Override
	public String getScheme() {
		return SCHEME;
	}

	/**
	 * Return the underlying protocol that is used to talk to the namenode.
	 */
	@Override
	protected String getUnderlyingProtocol() {
		return "https";
	}

	@Override
	protected void initTokenAspect() {
		tokenAspect = new TokenAspect<HsftpFileSystem>(this, tokenServiceName, TOKEN_KIND);
	}

	@Override
	public int getDefaultPort() {
		String DFS_NAMENODE_HTTP_PORT_KEY = "dfs.http.port";
		int DFS_NAMENODE_HTTP_PORT_DEFAULT = 50070;
		return getConf().getInt(DFS_NAMENODE_HTTP_PORT_KEY, DFS_NAMENODE_HTTP_PORT_DEFAULT);
	}

}

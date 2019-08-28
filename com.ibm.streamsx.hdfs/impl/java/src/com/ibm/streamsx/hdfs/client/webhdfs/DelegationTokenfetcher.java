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

 /**
 * This java source file is originally from org.apache.hadoop.hdfs.web package
 * It is modified to support HttpURLConnection with knox password and get WebHdfsFileSystem
 */

package com.ibm.streamsx.hdfs.client.webhdfs;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.Token;

import com.google.common.base.Charsets;

/**
 * Fetch a DelegationToken from the current Namenode and store it in the
 * specified file.
 */
@InterfaceAudience.Private
public class DelegationTokenfetcher extends DelegationTokenFetcher {
	private static final Log LOG = LogFactory.getLog(DelegationTokenfetcher.class);
	private static final String RENEWER = "renewer";
	private static final String PATH_SPEC = "/imagetransfer";
	private static final String TOKEN = "token";

	static public Credentials getDTfromRemote(URLConnectionFactory factory, URI nnUri, String renewer, String proxyUser)
			throws IOException {
		StringBuilder buf = new StringBuilder(nnUri.toString()).append(PATH_SPEC);
		String separator = "?";
		if (renewer != null) {
			buf.append("?").append(RENEWER).append("=").append(renewer);
			separator = "&";
		}
		if (proxyUser != null) {
			buf.append(separator).append("doas=").append(proxyUser);
		}

		boolean isHttps = nnUri.getScheme().equals("https");

		HttpURLConnection conn = null;
		DataInputStream dis = null;
		InetSocketAddress serviceAddr = NetUtils.createSocketAddr(nnUri.getAuthority());

		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Retrieving token from: " + buf);
			}

			conn = run(factory, new URL(buf.toString()));
			InputStream in = conn.getInputStream();
			Credentials ts = new Credentials();
			dis = new DataInputStream(in);
			ts.readFields(dis);
			for (Token<?> token : ts.getAllTokens()) {
				token.setKind(isHttps ? HsftpFileSystem.TOKEN_KIND : HftpFileSystem.TOKEN_KIND);
				SecurityUtil.setTokenService(token, serviceAddr);
			}
			return ts;
		} catch (Exception e) {
			throw new IOException("Unable to obtain remote token", e);
		} finally {
			IOUtils.cleanup(LOG, dis);
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	private static HttpURLConnection run(URLConnectionFactory factory, URL url) throws IOException,
			AuthenticationException {
		HttpURLConnection conn = null;

		try {
			conn = (HttpURLConnection) factory.openConnection(url, true);
			if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
				String msg = conn.getResponseMessage();

				throw new IOException("Error when dealing remote token: " + msg);
			}
		} catch (IOException ie) {
			LOG.info("Error when dealing remote token:", ie);
			IOException e = getExceptionFromResponse(conn);

			if (e != null) {
				LOG.info("rethrowing exception from HTTP request: " + e.getLocalizedMessage());
				throw e;
			}
			throw ie;
		}
		return conn;
	}

	// parse the message and extract the name of the exception and the message
	static private IOException getExceptionFromResponse(HttpURLConnection con) {
		IOException e = null;
		String resp;
		if (con == null)
			return null;

		try {
			resp = con.getResponseMessage();
		} catch (IOException ie) {
			return null;
		}
		if (resp == null || resp.isEmpty())
			return null;

		String exceptionClass = "", exceptionMsg = "";
		String[] rs = resp.split(";");
		if (rs.length < 2)
			return null;
		exceptionClass = rs[0];
		exceptionMsg = rs[1];
		LOG.info("Error response from HTTP request=" + resp + ";ec=" + exceptionClass + ";em=" + exceptionMsg);

		if (exceptionClass == null || exceptionClass.isEmpty())
			return null;

		// recreate exception objects
		try {
			Class<? extends Exception> ec = Class.forName(exceptionClass).asSubclass(Exception.class);
			// we are interested in constructor with String arguments
			java.lang.reflect.Constructor<? extends Exception> constructor = ec.getConstructor(new Class[] {
					String.class });

			// create an instance
			e = (IOException) constructor.newInstance(exceptionMsg);

		} catch (Exception ee) {
			LOG.warn("failed to create object of this class", ee);
		}
		if (e == null)
			return null;

		e.setStackTrace(new StackTraceElement[0]); // local stack is not
													 // relevant
		LOG.info("Exception from HTTP response=" + e.getLocalizedMessage());
		return e;
	}

	/**
	 * Renew a Delegation Token.
	 * 
	 * @param nnAddr the NameNode's address
	 * @param tok the token to renew
	 * @return the Date that the token will expire next.
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	static public long renewDelegationToken(URLConnectionFactory factory, URI nnAddr,
			Token<DelegationTokenIdentifier> tok) throws IOException, AuthenticationException {
		StringBuilder buf = new StringBuilder(nnAddr.toString()).append(PATH_SPEC).append("?").append(TOKEN).append("=")
				.append(tok.encodeToUrlString());

		HttpURLConnection connection = null;
		BufferedReader in = null;
		try {
			connection = run(factory, new URL(buf.toString()));
			in = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charsets.UTF_8));
			long result = Long.parseLong(in.readLine());
			return result;
		} catch (IOException ie) {
			LOG.info("error in renew over HTTP", ie);
			IOException e = getExceptionFromResponse(connection);

			if (e != null) {
				LOG.info("rethrowing exception from HTTP request: " + e.getLocalizedMessage());
				throw e;
			}
			throw ie;
		} finally {
			IOUtils.cleanup(LOG, in);
			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	/**
	 * Cancel a Delegation Token.
	 * 
	 * @param nnAddr the NameNode's address
	 * @param tok the token to cancel
	 * @throws IOException
	 * @throws AuthenticationException
	 */
	static public void cancelDelegationToken(URLConnectionFactory factory, URI nnAddr,
			Token<DelegationTokenIdentifier> tok) throws IOException, AuthenticationException {
		StringBuilder buf = new StringBuilder(nnAddr.toString()).append(PATH_SPEC).append("?").append(TOKEN).append("=")
				.append(tok.encodeToUrlString());
		HttpURLConnection conn = run(factory, new URL(buf.toString()));
		conn.disconnect();
	}

}
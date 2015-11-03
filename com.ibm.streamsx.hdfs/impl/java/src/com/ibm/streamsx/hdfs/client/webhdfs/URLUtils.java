/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.streamsx.hdfs.client.webhdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import com.ibm.streamsx.hdfs.IHdfsConstants;

/**
 * Utilities for handling URLs
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public class URLUtils {
	/**
	 * Timeout for socket connects and reads
	 */
	public static int SOCKET_TIMEOUT = 1*60*1000; // 1 minute

	/**
	 * Opens a url with read and connect timeouts
	 * @param url to open
	 * @return URLConnection
	 * @throws IOException
	 */
	public static URLConnection openConnection(URL url) throws IOException {
		URLConnection connection = url.openConnection();
		setTimeouts(connection);
		return connection;    
	}

	/**
	 * Sets timeout parameters on the given URLConnection.
	 * 
	 * @param connection URLConnection to set
	 */
	static void setTimeouts(URLConnection connection) {
		connection.setConnectTimeout(SOCKET_TIMEOUT);
		connection.setReadTimeout(SOCKET_TIMEOUT);
	}
	private static TrustManager[] getAcceptAllTrustManager() {
		TrustManager[] trustAllCerts = new TrustManager[] {
				new X509TrustManager() {

					@Override
					public X509Certificate[] getAcceptedIssuers() {
						return null;
					}

					@Override
					public void checkServerTrusted(X509Certificate[] arg0, String arg1)
							throws CertificateException {
					}

					@Override
					public void checkClientTrusted(X509Certificate[] arg0, String arg1)
							throws CertificateException {
					}
				}	
		};
		return trustAllCerts;
	}
	public static SSLSocketFactory getSocketFactory(String keyStore,
			String keyStorePassword) throws Exception {

		SSLContext sc = SSLContext.getInstance("TLS");
		TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		TrustManager[] managerArray = null;
		KeyStore userKeyStore = null;
		if (keyStore != null) {
			userKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
			//load the user key store
			userKeyStore.load(new FileInputStream(keyStore),   keyStorePassword.toCharArray());
			factory.init(userKeyStore);
			managerArray = factory.getTrustManagers();
		} else {
			//no store specified, just don't authenticate then.
			managerArray = getAcceptAllTrustManager();
		}
		sc.init(null, managerArray, null);
		return sc.getSocketFactory();
	}
}

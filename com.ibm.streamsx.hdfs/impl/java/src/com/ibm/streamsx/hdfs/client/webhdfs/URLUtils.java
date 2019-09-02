/*******************************************************************************
 * Copyright (C) 2017-2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.hdfs.client.webhdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

public class URLUtils {
	/**
	 * Timeout for socket connects and reads
	 */
	public static int TIMEOUT = 1 * 60 * 1000; // 1 minute

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
		connection.setConnectTimeout(TIMEOUT);
		connection.setReadTimeout(TIMEOUT);
	}

	private static TrustManager[] getAcceptAllTrustManager() {
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {

			@Override
			public X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			@Override
			public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
			}

			@Override
			public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
			}
		} };
		return trustAllCerts;
	}

	/**
	 * Creates a socket factory that is configured to use the keystore indicated by the
	 * 
	 * @param keyStore and
	 * @param keyStorePassword parameters.
	 */
	public static SSLSocketFactory getSocketFactory(String keyStore, String keyStorePassword) throws Exception {

		SSLContext sc = SSLContext.getInstance("TLS");
		TrustManagerFactory factory = TrustManagerFactory.getInstance("PKIX");
		TrustManager[] managerArray = null;
		KeyStore userKeyStore = null;
		if (keyStore != null) {
			userKeyStore = KeyStore.getInstance("JKS");
			// load the user key store
			userKeyStore.load(new FileInputStream(keyStore), keyStorePassword.toCharArray());
			factory.init(userKeyStore);
			managerArray = factory.getTrustManagers();
		} else {
			// no store specified, just don't authenticate then.
			managerArray = getAcceptAllTrustManager();
		}
		sc.init(null, managerArray, null);
		return sc.getSocketFactory();
	}
}

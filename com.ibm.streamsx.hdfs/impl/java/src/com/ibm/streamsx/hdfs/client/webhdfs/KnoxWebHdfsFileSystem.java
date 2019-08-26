package com.ibm.streamsx.hdfs.client.webhdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam.Op;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;

import com.ibm.streamsx.hdfs.IHdfsConstants;

public class KnoxWebHdfsFileSystem extends SWebHdfsFileSystem {

	// Encoded representation of the username/password credentials
	// sent via the Authorization header.
	private String authString;

	public static final String GATEWAY = "gateway/default";

	/** knox http URI: https://namenode:port/{PATH_PREFIX}/path/to/file */
	public static final String PREFIX = "/" + GATEWAY + "/webhdfs/v" + VERSION;

	public KnoxWebHdfsFileSystem() {
	}

	protected String getPathPrefix() {
		return PREFIX;
	}

	@Override
	protected URL toUrl(Op op, Path fspath, Param<?, ?>... parameters) throws IOException {
		// initialize URI path and query
		final String path = getPathPrefix() + (fspath == null ? "/" : makeQualified(fspath).toUri().getRawPath());
		final String query = op.toQueryString() + (authString == null ? Param.toSortedString("&", getAuthParameters(op))
				: "") + Param.toSortedString("&", parameters);
		// final URL url = getNamenodeURL(path, query);
		final URL url = getNamenodeURL(path, query);
		if (LOG.isTraceEnabled()) {
			LOG.trace("url=" + url);
		}
		return url;
	}

	@Override
	public String getScheme() {
		// even though we're using SSL the default Knox scheme for IBM Cloud is
		// still webhdfs
		return "webhdfs";
	}

	@Override
	public synchronized void initialize(URI uri, Configuration configuration) throws IOException {
		super.initialize(uri, configuration);

		String knox_password = configuration.get(IHdfsConstants.KNOX_PASSWORD);
		String knox_user = configuration.get(IHdfsConstants.KNOX_USER);
		if (isValid(knox_user) && isValid(knox_password)) {
			authString = getAuthStringForRequest(knox_user, knox_password);
			configureCertValidation();

		}
		// we use our own URL connection factory to allow us to set the Auth
		// parameters
		ConnectionConfigurator configurator = URLConnectionFactory.DEFAULT_TIMEOUT_CONN_CONFIGURATOR;
		connectionFactory = new URLConnectionFactory(configurator) {
			@Override
			public URLConnection openConnection(URL url) throws IOException {
				URLConnection conn = URLUtils.openConnection(url);
				conn.setRequestProperty("Authorization", authString);
				return conn;
			}

			@Override
			public URLConnection openConnection(URL url, boolean arg1) throws IOException, AuthenticationException {
				URLConnection conn = URLUtils.openConnection(url);
				conn.setRequestProperty("Authorization", authString);
				return conn;
			}
		};
	}

	private void configureCertValidation() {
		Configuration conf = getConf();
		// if the keystore and its password aren't specified, we assume that the
		// user is not interested in certificate validation.
		String keyStore = conf.get(IHdfsConstants.KEYSTORE);
		String keyStorePassword = conf.get(IHdfsConstants.KEYSTORE_PASSWORD, "");
		SSLSocketFactory factory;
		try {
			factory = URLUtils.getSocketFactory(keyStore, keyStorePassword);
			HttpsURLConnection.setDefaultSSLSocketFactory(factory);
		} catch (Exception e) {
			LOG.error("Error configuring SSL Connection ", e);
		}

		HostnameVerifier def = new HostnameVerifier() {

			@Override
			public boolean verify(String host, SSLSession session) {
				// boolean valid = SSLHostnameVerifier.DEFAULT.verify(host,
				// session);
				boolean valid = true;
				if (!valid) {
					LOG.warn("Cannot verify host " + host);

					return true;
				}
				return valid;
			}
		};

		HttpsURLConnection.setDefaultHostnameVerifier(def);
	}

	private boolean isValid(String value) {
		return value != null && !value.isEmpty();
	}

	/**
	 * Get the Base64 encoded basic authorization string for the given username
	 * and password
	 * 
	 * @param user username for authentication
	 * @param password password for the given user.
	 * @return The Base64 encoded authorization string
	 * @throws IOException
	 */
	private String getAuthStringForRequest(String user, String password) {

		String encodedString = "Basic " + Base64.encodeBase64String((user + ":" + password).getBytes()).replaceAll("\n",
				"").replaceAll("\r", "");

		return encodedString;
	}

}

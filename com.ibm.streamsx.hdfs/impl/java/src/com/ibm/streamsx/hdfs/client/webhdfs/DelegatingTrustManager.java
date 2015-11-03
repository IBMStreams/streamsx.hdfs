package com.ibm.streamsx.hdfs.client.webhdfs;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
/**
 * This class is used to first delegate the certificate validation to the default JVM trust manager,
 * and then check the trust store provided by the user if one exists.  
**/
public class DelegatingTrustManager implements X509TrustManager {

	private X509TrustManager fDelegateManager; //default trust manager 
	private X509TrustManager fUserStoreMgr; //additional certificates provided by user

	
	public  DelegatingTrustManager() throws Exception {
		//no user store specified, thus just go with the default user store 
		TrustManagerFactory factory  =
				TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		factory.init((KeyStore)null);
		fDelegateManager = getFirstManager(factory);
	}

	public  DelegatingTrustManager(String trustStorePath, String password) throws Exception {
	
		this();
		if (fDelegateManager == null){
			throw new Exception("Problem loading default keystore");
		}
		KeyStore userKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
		//load the user key store
		userKeyStore.load(new FileInputStream(trustStorePath),   password.toCharArray());
		TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		factory.init(userKeyStore);
		//get a trust manager that uses the given key store
		fUserStoreMgr = getFirstManager(factory);
	}
	private X509TrustManager getFirstManager(TrustManagerFactory factory) {
		
		for (TrustManager mgr:  factory.getTrustManagers()){
			if (mgr instanceof X509TrustManager) {
				return (X509TrustManager) mgr;
			}
		}
		return null;
	}

	 public void checkClientTrusted(X509Certificate[] chain, String authType)
			 throws CertificateException {
		try {
			fDelegateManager.checkClientTrusted(chain, authType);
		} catch (CertificateException excep) {
			if (fUserStoreMgr != null){
				//if the user specified a trust store, we'll check it,
				//otherwise we'll ignore the invalid ceritifcate
				fUserStoreMgr.checkClientTrusted(chain, authType);
			}
		}
	 }

	 public void checkServerTrusted(X509Certificate[] chain, String authType)
			 throws CertificateException {
		 try {
			 fDelegateManager.checkServerTrusted(chain, authType);
		 } catch (CertificateException excep) {
			 //certificate not valid, check the user specified keystore, if it exists,
				//otherwise we'll ignore the invalid ceritifcate

			if (fUserStoreMgr != null){
				fUserStoreMgr.checkServerTrusted(chain, authType);
			}
		 }
	 }

	 public X509Certificate[] getAcceptedIssuers() {
		 
		 return fDelegateManager.getAcceptedIssuers();
	 }
}
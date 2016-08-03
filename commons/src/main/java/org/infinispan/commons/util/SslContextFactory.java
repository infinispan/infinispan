package org.infinispan.commons.util;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

/**
 * SslContextFactory.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslContextFactory {
   private static final Log log = LogFactory.getLog(SslContextFactory.class);
   private static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2";
   private static final String CLASSPATH_RESOURCE = "classpath:";

   public static SSLContext getContext(String keyStoreFileName, char[] keyStorePassword, String trustStoreFileName, char[] trustStorePassword) {
      return getContext(keyStoreFileName, keyStorePassword, null, trustStoreFileName, trustStorePassword, DEFAULT_SSL_PROTOCOL);
   }

   public static SSLContext getContext(String keyStoreFileName, char[] keyStorePassword, String trustStoreFileName, char[] trustStorePassword, String sslProtocol) {
      return getContext(keyStoreFileName, keyStorePassword, null, trustStoreFileName, trustStorePassword, sslProtocol);
   }

   public static SSLContext getContext(String keyStoreFileName, char[] keyStorePassword, char[] keyStoreCertificatePassword, String trustStoreFileName, char[] trustStorePassword) {
      return getContext(keyStoreFileName, keyStorePassword, keyStoreCertificatePassword, trustStoreFileName, trustStorePassword, DEFAULT_SSL_PROTOCOL);
   }

   public static SSLContext getContext(String keyStoreFileName, char[] keyStorePassword, char[] keyStoreCertificatePassword,
                                       String trustStoreFileName, char[] trustStorePassword, String sslProtocol) {
      return getContext(keyStoreFileName, keyStorePassword, keyStoreCertificatePassword, trustStoreFileName, trustStorePassword, DEFAULT_SSL_PROTOCOL, null);
   }

   public static SSLContext getContext(String keyStoreFileName, char[] keyStorePassword, char[] keyStoreCertificatePassword,
                                       String trustStoreFileName, char[] trustStorePassword, String sslProtocol, ClassLoader classLoader) {
      try {
         KeyManager[] keyManagers = null;
         if (keyStoreFileName != null) {
            KeyStore ks = KeyStore.getInstance("JKS");
            loadKeyStore(ks, keyStoreFileName, keyStorePassword, classLoader);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keyStoreCertificatePassword == null ? keyStorePassword : keyStoreCertificatePassword);
            keyManagers = kmf.getKeyManagers();
         }

         TrustManager[] trustManagers = null;
         if (trustStoreFileName != null) {
            KeyStore ks = KeyStore.getInstance("JKS");
            loadKeyStore(ks, trustStoreFileName, trustStorePassword, classLoader);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            trustManagers = tmf.getTrustManagers();
         }

         SSLContext sslContext = SSLContext.getInstance(sslProtocol == null ? DEFAULT_SSL_PROTOCOL : sslProtocol);
         sslContext.init(keyManagers, trustManagers, null);
         return sslContext;
      } catch (Exception e) {
         throw log.sslInitializationException(e);
      }
   }

   public static SSLEngine getEngine(SSLContext sslContext, boolean useClientMode, boolean needClientAuth) {
      SSLEngine sslEngine = sslContext.createSSLEngine();
      sslEngine.setUseClientMode(useClientMode);
      sslEngine.setNeedClientAuth(needClientAuth);
      return sslEngine;
   }

   private static void loadKeyStore(KeyStore ks, String keyStoreFileName, char[] keyStorePassword, ClassLoader classLoader) throws IOException, GeneralSecurityException {
      InputStream is = null;
      try {
         if (keyStoreFileName.startsWith(CLASSPATH_RESOURCE)) {
            String fileName = keyStoreFileName.substring(keyStoreFileName.indexOf(":") + 1);
            is = Util.getResourceAsStream(fileName, classLoader);
            if (is == null) {
               throw log.cannotFindResource(keyStoreFileName);
            }
         } else {
            is = new BufferedInputStream(new FileInputStream(keyStoreFileName));
         }
         ks.load(is, keyStorePassword);
      } finally {
         Util.close(is);
      }
   }

}

package org.infinispan.commons.util;

import static org.infinispan.commons.logging.Log.SECURITY;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.wildfly.openssl.OpenSSLProvider;

/**
 * SslContextFactory.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslContextFactory {
   private static final String DEFAULT_KEYSTORE_TYPE = "JKS";
   private static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2";
   private static final String CLASSPATH_RESOURCE = "classpath:";
   private static final String SSL_PROTOCOL_PREFIX;

   static {
      String sslProtocolPrefix = "";
      if (Boolean.parseBoolean(SecurityActions.getProperty("org.infinispan.openssl", "true"))) {
         try {
            OpenSSLProvider.register();
            sslProtocolPrefix = "openssl.";
            SECURITY.openSSLAvailable();
         } catch (Throwable e) {
            SECURITY.openSSLNotAvailable();
         }
      }
      SSL_PROTOCOL_PREFIX = sslProtocolPrefix;
   }

   private String keyStoreFileName;
   private char[] keyStorePassword;
   private char[] keyStoreCertificatePassword;
   private String keyStoreType = DEFAULT_KEYSTORE_TYPE;
   private String keyAlias;
   private String trustStoreFileName;
   private char[] trustStorePassword;
   private String trustStoreType = DEFAULT_KEYSTORE_TYPE;
   private String sslProtocol = DEFAULT_SSL_PROTOCOL;
   private boolean useNativeIfAvailable = true;
   private ClassLoader classLoader;

   public SslContextFactory() {
   }

   public SslContextFactory keyStoreFileName(String keyStoreFileName) {
      this.keyStoreFileName = keyStoreFileName;
      return this;
   }

   public SslContextFactory keyStorePassword(char[] keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
   }

   public SslContextFactory keyStoreCertificatePassword(char[] keyStoreCertificatePassword) {
      this.keyStoreCertificatePassword = keyStoreCertificatePassword;
      return this;
   }

   public SslContextFactory keyStoreType(String keyStoreType) {
      if (keyStoreType != null) {
         this.keyStoreType = keyStoreType;
      }
      return this;
   }

   public SslContextFactory keyAlias(String keyAlias) {
      this.keyAlias = keyAlias;
      return this;
   }

   public SslContextFactory trustStoreFileName(String trustStoreFileName) {
      this.trustStoreFileName = trustStoreFileName;
      return this;
   }

   public SslContextFactory trustStorePassword(char[] trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return this;
   }

   public SslContextFactory trustStoreType(String trustStoreType) {
      if (trustStoreType != null) {
         this.trustStoreType = trustStoreType;
      }
      return this;
   }

   public SslContextFactory sslProtocol(String sslProtocol) {
      if (sslProtocol != null) {
         this.sslProtocol = sslProtocol;
      }
      return this;
   }

   public SslContextFactory useNativeIfAvailable(boolean useNativeIfAvailable) {
      this.useNativeIfAvailable = useNativeIfAvailable;
      return this;
   }

   public SslContextFactory classLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
   }

   public SSLContext getContext() {
      try {
         KeyManager[] keyManagers = null;
         if (keyStoreFileName != null) {
            KeyManagerFactory kmf = getKeyManagerFactory();
            keyManagers = kmf.getKeyManagers();
         }
         TrustManager[] trustManagers = null;
         if (trustStoreFileName != null) {
            TrustManagerFactory tmf = getTrustManagerFactory();
            trustManagers = tmf.getTrustManagers();
         }
         String protocol = useNativeIfAvailable ? SSL_PROTOCOL_PREFIX + sslProtocol : sslProtocol;
         SSLContext sslContext = SSLContext.getInstance(protocol);
         sslContext.init(keyManagers, trustManagers, null);
         return sslContext;
      } catch (Exception e) {
         throw SECURITY.sslInitializationException(e);
      }
   }

   public KeyManagerFactory getKeyManagerFactory() throws IOException, GeneralSecurityException {
      KeyStore ks = KeyStore.getInstance(keyStoreType != null ? keyStoreType : DEFAULT_KEYSTORE_TYPE);
      loadKeyStore(ks, keyStoreFileName, keyStorePassword, classLoader);
      char[] keyPassword = keyStoreCertificatePassword == null ? keyStorePassword : keyStoreCertificatePassword;
      if (keyAlias != null) {
         if (ks.containsAlias(keyAlias) && ks.isKeyEntry(keyAlias)) {
            KeyStore.PasswordProtection passParam = new KeyStore.PasswordProtection(keyPassword);
            KeyStore.Entry entry = ks.getEntry(keyAlias, passParam);
            // Recreate the keystore with just one key
            ks = KeyStore.getInstance(keyStoreType != null ? keyStoreType : DEFAULT_KEYSTORE_TYPE);
            ks.load(null);
            ks.setEntry(keyAlias, entry, passParam);
         } else {
            throw SECURITY.noSuchAliasInKeyStore(keyAlias, keyStoreFileName);
         }
      }
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, keyPassword);
      return kmf;
   }

   public TrustManagerFactory getTrustManagerFactory() throws IOException, GeneralSecurityException {
      KeyStore ks = KeyStore.getInstance(trustStoreType != null ? trustStoreType : DEFAULT_KEYSTORE_TYPE);
      loadKeyStore(ks, trustStoreFileName, trustStorePassword, classLoader);
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      return tmf;
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
               throw SECURITY.cannotFindResource(keyStoreFileName);
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

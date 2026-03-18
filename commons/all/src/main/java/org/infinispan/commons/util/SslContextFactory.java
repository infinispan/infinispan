package org.infinispan.commons.util;

import static org.infinispan.commons.logging.Log.SECURITY;
import static org.infinispan.commons.util.SecurityProviders.findProvider;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.infinispan.commons.io.FileWatcher;


/**
 * SslContextFactory.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslContextFactory {
   private static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";
   private static final String DEFAULT_SSL_PROTOCOL = "TLSv1.3";
   private static final String CLASSPATH_RESOURCE = "classpath:";

   private String keyStoreFileName;
   private char[] keyStorePassword;
   private String keyStoreType = DEFAULT_KEYSTORE_TYPE;
   private String keyAlias;
   private String trustStoreFileName;
   private char[] trustStorePassword;
   private String trustStoreType = DEFAULT_KEYSTORE_TYPE;
   private String sslProtocol = DEFAULT_SSL_PROTOCOL;
   private ClassLoader classLoader;
   private String provider;
   private FileWatcher watcher;

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

   public SslContextFactory provider(String provider) {
      if (provider != null) {
         this.provider = provider;
      }
      return this;
   }

   public SslContextFactory watcher(FileWatcher watcher) {
      if (watcher != null) {
         this.watcher = watcher;
      }
      return this;
   }

   @Deprecated(forRemoval = true, since = "15.0")
   public SslContextFactory useNativeIfAvailable(boolean useNativeIfAvailable) {
      return this;
   }

   public SslContextFactory classLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
   }

   public Context build() {
      try {
         KeyManager[] kms = null;
         if (keyStoreFileName != null) {
            if (watcher != null) {
               kms = new KeyManager[]{new ReloadingX509KeyManager(watcher, Path.of(keyStoreFileName), p -> getKeyManager())};
            } else {
               kms = new KeyManager[]{getKeyManager()};
            }
         }
         TrustManager[] tms = null;
         if (trustStoreFileName != null) {
            if (watcher != null) {
               tms = new TrustManager[]{new ReloadingX509TrustManager(watcher, Path.of(trustStoreFileName), p -> getTrustManager())};
            } else {
               tms = new TrustManager[]{getTrustManager()};
            }
         }
         SSLContext sslContext;
         Provider provider = null;
         if (this.provider != null) {
            // If the user has supplied a provider, try to use it
            provider = findProvider(this.provider, SSLContext.class.getSimpleName(), sslProtocol);
         }
         sslContext = provider != null ? SSLContext.getInstance(sslProtocol, provider) : SSLContext.getInstance(sslProtocol);
         sslContext.init(kms, tms, null);
         return new Context(sslContext, kms != null ? kms[0] : null, tms != null ? tms[0] : null);
      } catch (Exception e) {
         throw SECURITY.sslInitializationException(e);
      }
   }

   private X509ExtendedKeyManager getKeyManager() {
      try {
         KeyStore ks;
         File keyStoreFile = new File(keyStoreFileName);
         if (isPemFile(keyStoreFile)) {
            ks = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
            ks.load(null, null);
            PrivateKey key = readPemKey(keyStoreFile);
            X509Certificate[] certs = readPem(keyStoreFile);
            String alias = keyAlias != null ? keyAlias : "key";
            ks.setKeyEntry(alias, key, keyStorePassword, certs);
         } else {
            String type = keyStoreType != null ? keyStoreType : DEFAULT_KEYSTORE_TYPE;
            Provider provider = findProvider(this.provider, KeyManagerFactory.class.getSimpleName(), type);
            ks = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
            loadKeyStore(ks, keyStoreFileName, keyStorePassword, classLoader);
            if (keyAlias != null) {
               if (ks.containsAlias(keyAlias) && ks.isKeyEntry(keyAlias)) {
                  KeyStore.PasswordProtection passParam = new KeyStore.PasswordProtection(keyStorePassword);
                  KeyStore.Entry entry = ks.getEntry(keyAlias, passParam);
                  // Recreate the keystore with just one key
                  ks = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
                  ks.load(null, null);
                  ks.setEntry(keyAlias, entry, passParam);
               } else {
                  throw SECURITY.noSuchAliasInKeyStore(keyAlias, keyStoreFileName);
               }
            }
         }
         String algorithm = KeyManagerFactory.getDefaultAlgorithm();
         Provider provider = findProvider(this.provider, KeyManagerFactory.class.getSimpleName(), algorithm);
         KeyManagerFactory kmf = provider != null ? KeyManagerFactory.getInstance(algorithm, provider) : KeyManagerFactory.getInstance(algorithm);
         kmf.init(ks, keyStorePassword);
         for (KeyManager km : kmf.getKeyManagers()) {
            if (km instanceof X509ExtendedKeyManager xkm) {
               return xkm;
            }
         }
         throw new GeneralSecurityException("Could not obtain an X509ExtendedKeyManager");
      } catch (GeneralSecurityException | IOException e) {
         throw SECURITY.sslInitializationException(e);
      }
   }

   private X509ExtendedTrustManager getTrustManager() {
      try {
         KeyStore ks;
         File trustStoreFile = new File(trustStoreFileName);
         if (isPemFile(trustStoreFile)) {
            ks = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
            ks.load(null, null);
            X509Certificate[] certs = readPem(trustStoreFile);
            for (int i = 0; i < certs.length; i++) {
               ks.setCertificateEntry("cert-" + i, certs[i]);
            }
         } else {
            String type = trustStoreType != null ? trustStoreType : DEFAULT_KEYSTORE_TYPE;
            Provider provider = findProvider(this.provider, KeyStore.class.getSimpleName(), trustStoreType);
            ks = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
            loadKeyStore(ks, trustStoreFileName, trustStorePassword, classLoader);
         }
         String algorithm = KeyManagerFactory.getDefaultAlgorithm();
         Provider provider = findProvider(this.provider, TrustManagerFactory.class.getSimpleName(), algorithm);
         TrustManagerFactory tmf = provider != null ? TrustManagerFactory.getInstance(algorithm, provider) : TrustManagerFactory.getInstance(algorithm);
         tmf.init(ks);
         for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509ExtendedTrustManager xkm) {
               return xkm;
            }
         }
         throw new GeneralSecurityException("Could not obtain an X509TrustManager");
      } catch (GeneralSecurityException | IOException e) {
         throw SECURITY.sslInitializationException(e);
      }
   }

   @Deprecated(forRemoval = true, since = "15.0")
   public static String getSslProvider() {
      return null;
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

   public static X509Certificate[] readPem(File file) throws IOException, GeneralSecurityException {
      StringBuilder sb = new StringBuilder();
      boolean inCert = false;
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
         String line;
         while ((line = reader.readLine()) != null) {
            if (line.startsWith("-----BEGIN CERTIFICATE")) {
               inCert = true;
            }
            if (inCert) {
               sb.append(line).append('\n');
            }
            if (line.startsWith("-----END CERTIFICATE")) {
               inCert = false;
            }
         }
      }
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      try (InputStream is = new java.io.ByteArrayInputStream(sb.toString().getBytes())) {
         return cf.generateCertificates(is).toArray(new X509Certificate[0]);
      }
   }

   public static PrivateKey readPemKey(File file) throws IOException, GeneralSecurityException {
      StringBuilder sb = new StringBuilder();
      boolean inKey = false;
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
         String line;
         while ((line = reader.readLine()) != null) {
            if (line.startsWith("-----BEGIN") && line.contains("PRIVATE KEY")) {
               inKey = true;
            } else if (line.startsWith("-----END") && line.contains("PRIVATE KEY")) {
               break;
            } else if (inKey) {
               sb.append(line);
            }
         }
      }
      byte[] keyBytes = Base64.getDecoder().decode(sb.toString());
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
      try {
         return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
      } catch (GeneralSecurityException e) {
         return KeyFactory.getInstance("EC").generatePrivate(keySpec);
      }
   }

   public static boolean isPemFile(File file) {
      try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
         byte[] header = new byte[5];
         int read = is.read(header);
         return read == 5 && new String(header).equals("-----");
      } catch (IOException e) {
         return false;
      }
   }

   public record Context(SSLContext sslContext, KeyManager keyManager, TrustManager trustManager) {
   }
}

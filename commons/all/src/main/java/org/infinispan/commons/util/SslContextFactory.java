package org.infinispan.commons.util;

import static org.infinispan.commons.logging.Log.SECURITY;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.wildfly.openssl.OpenSSLProvider;
import org.wildfly.openssl.SSL;

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
   private static final String SSL_PROVIDER;
   private static final ConcurrentHashMap<ClassLoader, Provider[]> PER_CLASSLOADER_PROVIDERS = new ConcurrentHashMap<>(2);

   static {
      String sslProvider = null;
      if (Boolean.parseBoolean(SecurityActions.getProperty("org.infinispan.openssl", "true"))) {
         try {
            OpenSSLProvider.register();
            SSL.getInstance();
            sslProvider = "openssl";
            SECURITY.openSSLAvailable();
         } catch (Throwable e) {
            SECURITY.openSSLNotAvailable();
         }
      }
      SSL_PROVIDER = sslProvider;
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
   private String provider;

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

   public SslContextFactory provider(String provider) {
      if (provider != null) {
         this.provider = provider;
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
         SSLContext sslContext;
         Provider provider = null;
         if (this.provider != null) {
            // If the user has supplied a provider, try to use it
            provider = findProvider(this.provider, SSLContext.class.getSimpleName(), sslProtocol);
         }
         if (provider == null && useNativeIfAvailable && SSL_PROVIDER != null) {
            // Try to use the native provider if possible
            provider = findProvider(SSL_PROVIDER, SSLContext.class.getSimpleName(), sslProtocol);
         }
         sslContext = provider != null ? SSLContext.getInstance(sslProtocol, provider) : SSLContext.getInstance(sslProtocol);
         sslContext.init(keyManagers, trustManagers, null);
         return sslContext;
      } catch (Exception e) {
         throw SECURITY.sslInitializationException(e);
      }
   }

   public KeyManagerFactory getKeyManagerFactory() throws IOException, GeneralSecurityException {
      String type = keyStoreType != null ? keyStoreType : DEFAULT_KEYSTORE_TYPE;
      Provider provider = findProvider(this.provider, KeyManagerFactory.class.getSimpleName(), type);
      KeyStore ks = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
      loadKeyStore(ks, keyStoreFileName, keyStorePassword, classLoader);
      char[] keyPassword = keyStoreCertificatePassword == null ? keyStorePassword : keyStoreCertificatePassword;
      if (keyAlias != null) {
         if (ks.containsAlias(keyAlias) && ks.isKeyEntry(keyAlias)) {
            KeyStore.PasswordProtection passParam = new KeyStore.PasswordProtection(keyPassword);
            KeyStore.Entry entry = ks.getEntry(keyAlias, passParam);
            // Recreate the keystore with just one key
            ks = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
            ks.load(null, null);
            ks.setEntry(keyAlias, entry, passParam);
         } else {
            throw SECURITY.noSuchAliasInKeyStore(keyAlias, keyStoreFileName);
         }
      }
      String algorithm = KeyManagerFactory.getDefaultAlgorithm();
      provider = findProvider(this.provider, KeyManagerFactory.class.getSimpleName(), algorithm);
      KeyManagerFactory kmf = provider != null ? KeyManagerFactory.getInstance(algorithm, provider) : KeyManagerFactory.getInstance(algorithm);
      kmf.init(ks, keyPassword);
      return kmf;
   }

   public TrustManagerFactory getTrustManagerFactory() throws IOException, GeneralSecurityException {
      String type = trustStoreType != null ? trustStoreType : DEFAULT_KEYSTORE_TYPE;
      Provider provider = findProvider(this.provider, KeyStore.class.getSimpleName(), trustStoreType);
      KeyStore ks = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
      loadKeyStore(ks, trustStoreFileName, trustStorePassword, classLoader);
      String algorithm = KeyManagerFactory.getDefaultAlgorithm();
      provider = findProvider(this.provider, TrustManagerFactory.class.getSimpleName(), algorithm);
      TrustManagerFactory tmf = provider != null ? TrustManagerFactory.getInstance(algorithm, provider) : TrustManagerFactory.getInstance(algorithm);
      tmf.init(ks);
      return tmf;
   }

   public static String getSslProvider() {
      return SSL_PROVIDER;
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

   public static Provider findProvider(String providerName, String serviceType, String algorithm) {
      Provider[] providers = discoverSecurityProviders(Thread.currentThread().getContextClassLoader());
      for (Provider provider : providers) {
         if (providerName == null || providerName.equals(provider.getName())) {
            Provider.Service providerService = provider.getService(serviceType, algorithm);
            if (providerService != null) {
               return provider;
            }
         }
      }
      return null;
   }

   public static Provider[] discoverSecurityProviders(ClassLoader classLoader) {
      return PER_CLASSLOADER_PROVIDERS.computeIfAbsent(classLoader, cl -> {
               // We need to keep them sorted by insertion order, since we want system providers first
               Map<Class<? extends Provider>, Provider> providers = new LinkedHashMap<>();
               for (Provider provider : Security.getProviders()) {
                  providers.put(provider.getClass(), provider);
               }
               for (Provider provider : ServiceFinder.load(Provider.class, classLoader)) {
                  providers.putIfAbsent(provider.getClass(), provider);
               }
               return providers.values().toArray(new Provider[0]);
            }
      );
   }
}

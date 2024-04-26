package org.infinispan.commons.util;

import static org.infinispan.commons.logging.Log.SECURITY;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
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
   private static final ConcurrentHashMap<ClassLoader, Provider[]> PER_CLASSLOADER_PROVIDERS = new ConcurrentHashMap<>(2);

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
         String type = keyStoreType != null ? keyStoreType : DEFAULT_KEYSTORE_TYPE;
         Provider provider = findProvider(this.provider, KeyManagerFactory.class.getSimpleName(), type);
         KeyStore ks = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
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
         String algorithm = KeyManagerFactory.getDefaultAlgorithm();
         provider = findProvider(this.provider, KeyManagerFactory.class.getSimpleName(), algorithm);
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
         String type = trustStoreType != null ? trustStoreType : DEFAULT_KEYSTORE_TYPE;
         Provider provider = findProvider(this.provider, KeyStore.class.getSimpleName(), trustStoreType);
         KeyStore ks = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
         loadKeyStore(ks, trustStoreFileName, trustStorePassword, classLoader);
         String algorithm = KeyManagerFactory.getDefaultAlgorithm();
         provider = findProvider(this.provider, TrustManagerFactory.class.getSimpleName(), algorithm);
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

   public record Context(SSLContext sslContext, KeyManager keyManager, TrustManager trustManager) {
   }
}

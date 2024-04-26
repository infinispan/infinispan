package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.CredentialStoresConfiguration.resolvePassword;
import static org.infinispan.server.security.KeyStoreUtils.buildFilelessKeyStore;
import static org.wildfly.security.provider.util.ProviderUtil.findProvider;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.Provider;
import java.util.Properties;
import java.util.function.Supplier;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.io.FileWatcher;
import org.infinispan.commons.util.ReloadingX509TrustManager;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.configuration.ServerConfigurationSerializer;
import org.wildfly.security.credential.source.CredentialSource;
import org.wildfly.security.keystore.KeyStoreUtil;
import org.wildfly.security.ssl.SSLContextBuilder;

/**
 * @since 12.1
 */
public class TrustStoreConfiguration extends ConfigurationElement<TrustStoreConfiguration> {
   static final AttributeDefinition<Supplier<CredentialSource>> PASSWORD = AttributeDefinition.builder(Attribute.PASSWORD, null, (Class<Supplier<CredentialSource>>) (Class<?>) Supplier.class).serializer(ServerConfigurationSerializer.CREDENTIAL).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).autoPersist(false).build();
   static final AttributeDefinition<String> PROVIDER = AttributeDefinition.builder(Attribute.PROVIDER, null, String.class).build();
   static final AttributeDefinition<String> TYPE = AttributeDefinition.builder(Attribute.TYPE, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TrustStoreConfiguration.class, PATH, RELATIVE_TO, PROVIDER, PASSWORD, TYPE);
   }

   TrustStoreConfiguration(AttributeSet attributes) {
      super(Element.TRUSTSTORE, attributes);
   }

   KeyStore trustStore(Provider[] providers, Properties properties) {
      String fileName = ParseUtils.resolvePath(attributes.attribute(PATH).get(),
            properties.getProperty(attributes.attribute(RELATIVE_TO).get()));
      String providerName = attributes.attribute(PROVIDER).get();
      String type = attributes.attribute(TYPE).get();
      if (fileName == null) {
         try {
            return buildFilelessKeyStore(providers, providerName, type);
         } catch (GeneralSecurityException | IOException e) {
            throw new CacheConfigurationException(e);
         }
      } else {
         char[] password = resolvePassword(attributes.attribute(PASSWORD));
         try (FileInputStream is = new FileInputStream(fileName)) {
            return KeyStoreUtil.loadKeyStore(() -> providers, providerName, is, fileName, password);
         } catch (IOException | KeyStoreException e) {
            throw new CacheConfigurationException(e);
         }
      }
   }

   public void build(SSLContextBuilder builder, Properties properties) {
      if (attributes.isModified()) {
         Provider[] providers = SslContextFactory.discoverSecurityProviders(Thread.currentThread().getContextClassLoader());
         final X509TrustManager trustManager;
         if (attributes.attribute(PATH).isNull()) {
            try {
               String providerName = attributes.attribute(PROVIDER).get();
               String type = attributes.attribute(TYPE).get();
               KeyStore keyStore = buildFilelessKeyStore(providers, providerName, type);
               trustManager = trustManagerFromStore(keyStore, providers, providerName);
            } catch (GeneralSecurityException | IOException e) {
               throw new RuntimeException(e);
            }
         } else {
            String providerName = attributes.attribute(PROVIDER).get();
            String keyStoreFileName = ParseUtils.resolvePath(attributes.attribute(PATH).get(), properties.getProperty(attributes.attribute(RELATIVE_TO).get()));
            FileWatcher watcher = (FileWatcher) properties.get(Server.INFINISPAN_FILE_WATCHER);
            if (watcher == null) {
               try {
                  trustManager = trustManagerFromStore(trustStore(providers, properties), providers, providerName);
               } catch (GeneralSecurityException e) {
                  throw new RuntimeException(e);
               }
            } else {
               trustManager = new ReloadingX509TrustManager(watcher, Paths.get(keyStoreFileName), p -> {
                  try {
                     return trustManagerFromStore(trustStore(providers, properties), providers, providerName);
                  } catch (GeneralSecurityException e) {
                     throw new RuntimeException(e);
                  }
               });
            }
         }
         builder.setTrustManager(trustManager);
      }
   }

   private X509ExtendedTrustManager trustManagerFromStore(KeyStore keyStore, Provider[] providers, String providerName) throws GeneralSecurityException {
      String algorithm = TrustManagerFactory.getDefaultAlgorithm();
      Provider provider = findProvider(providers, providerName, KeyManagerFactory.class, algorithm);
      TrustManagerFactory trustManagerFactory = provider != null ? TrustManagerFactory.getInstance(algorithm, provider) : TrustManagerFactory.getInstance(algorithm);
      trustManagerFactory.init(keyStore);
      for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
         if (trustManager instanceof X509ExtendedTrustManager) {
            return (X509ExtendedTrustManager) trustManager;
         }
      }
      throw Server.log.noDefaultTrustManager();
   }
}

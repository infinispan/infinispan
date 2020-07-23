package org.infinispan.server.configuration.security;

import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.auth.realm.KeyStoreBackedSecurityRealm;
import org.wildfly.security.keystore.KeyStoreUtil;
import org.wildfly.security.provider.util.ProviderUtil;
import org.wildfly.security.ssl.SSLContextBuilder;

/**
 * @since 10.0
 */
public class TrustStoreRealmConfigurationBuilder implements Builder<TrustStoreRealmConfiguration> {
   private final AttributeSet attributes;
   private final RealmConfigurationBuilder securityRealmBuilder;
   private TrustManagerFactory trustManagerFactory;

   TrustStoreRealmConfigurationBuilder(RealmConfigurationBuilder securityRealmBuilder) {
      this.securityRealmBuilder = securityRealmBuilder;
      this.attributes = TrustStoreRealmConfiguration.attributeDefinitionSet();
   }

   public TrustStoreRealmConfigurationBuilder name(String name) {
      attributes.attribute(TrustStoreRealmConfiguration.NAME).set(name);
      return this;
   }

   public TrustStoreRealmConfigurationBuilder path(String path) {
      attributes.attribute(TrustStoreRealmConfiguration.PATH).set(path);
      return this;
   }

   public TrustStoreRealmConfigurationBuilder provider(String provider) {
      attributes.attribute(TrustStoreRealmConfiguration.PROVIDER).set(provider);
      return this;
   }

   public TrustStoreRealmConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      attributes.attribute(TrustStoreRealmConfiguration.KEYSTORE_PASSWORD).set(keyStorePassword);
      return this;
   }

   public TrustStoreRealmConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(TrustStoreRealmConfiguration.RELATIVE_TO).set(relativeTo);
      return this;
   }

   @Override
   public void validate() {
   }

   public TrustStoreRealmConfigurationBuilder build() {
      if (trustManagerFactory == null) {
         SSLContextBuilder sslContextBuilder = securityRealmBuilder.sslContextBuilder();
         if (sslContextBuilder == null) {
            throw Server.log.trustStoreWithoutServerIdentity();
         }
         String name = attributes.attribute(TrustStoreRealmConfiguration.NAME).get();
         String path = attributes.attribute(TrustStoreRealmConfiguration.PATH).get();
         String relativeTo = attributes.attribute(TrustStoreRealmConfiguration.RELATIVE_TO).get();
         String trustStoreFileName = ParseUtils.resolvePath(path, relativeTo);
         String keyStoreProvider = attributes.attribute(TrustStoreRealmConfiguration.PROVIDER).get();
         char[] keyStorePassword = attributes.attribute(TrustStoreRealmConfiguration.KEYSTORE_PASSWORD).get();
         try {
            KeyStore keyStore = KeyStoreUtil.loadKeyStore(ProviderUtil.INSTALLED_PROVIDERS, keyStoreProvider, new FileInputStream(trustStoreFileName), trustStoreFileName, keyStorePassword);
            trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);
            for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
               if (trustManager instanceof X509TrustManager) {
                  sslContextBuilder.setTrustManager((X509TrustManager) trustManager);
                  securityRealmBuilder.addRealm(name, new KeyStoreBackedSecurityRealm(keyStore));
                  securityRealmBuilder.addFeature(ServerSecurityRealm.Feature.TRUST);
                  break;
               }
            }
         } catch (Exception e) {
            throw new CacheConfigurationException(e);
         }
      }
      return this;
   }

   @Override
   public TrustStoreRealmConfiguration create() {
      return new TrustStoreRealmConfiguration(attributes.protect());
   }

   @Override
   public TrustStoreRealmConfigurationBuilder read(TrustStoreRealmConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}

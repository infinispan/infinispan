package org.infinispan.server.configuration.security;

import java.security.KeyStore;
import java.util.Properties;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.Server;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.auth.realm.KeyStoreBackedSecurityRealm;
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

   @Override
   public void validate() {
   }

   public TrustStoreRealmConfigurationBuilder build(Properties properties) {
      if (trustManagerFactory == null) {
         SSLContextBuilder sslContextBuilder = securityRealmBuilder.sslContextBuilder();
         if (sslContextBuilder == null) {
            throw Server.log.trustStoreWithoutServerIdentity();
         }
         String name = attributes.attribute(TrustStoreRealmConfiguration.NAME).get();
         try {
            KeyStore keyStore = securityRealmBuilder.serverIdentitiesConfiguration().sslConfiguration().trustStore().trustStore(properties);
            trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);
            for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
               if (trustManager instanceof X509TrustManager) {
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

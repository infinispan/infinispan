package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.TrustStoreConfiguration.PASSWORD;
import static org.infinispan.server.configuration.security.TrustStoreConfiguration.PATH;
import static org.infinispan.server.configuration.security.TrustStoreConfiguration.PROVIDER;
import static org.infinispan.server.configuration.security.TrustStoreConfiguration.RELATIVE_TO;
import static org.wildfly.security.provider.util.ProviderUtil.INSTALLED_PROVIDERS;

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
import org.wildfly.security.keystore.KeyStoreUtil;

/**
 * @since 12.1
 */
public class TrustStoreConfigurationBuilder implements Builder<TrustStoreConfiguration> {
   private final AttributeSet attributes;
   private final RealmConfigurationBuilder realmBuilder;
   private TrustManagerFactory trustManagerFactory;
   private KeyStore trustStore;

   TrustStoreConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
      this.attributes = TrustStoreConfiguration.attributeDefinitionSet();
   }

   public TrustStoreConfigurationBuilder password(char[] password) {
      attributes.attribute(PASSWORD).set(password);
      return this;
   }


   public TrustStoreConfigurationBuilder path(String path) {
      attributes.attribute(PATH).set(path);
      return this;
   }

   public TrustStoreConfigurationBuilder provider(String value) {
      attributes.attribute(PROVIDER).set(value);
      return this;
   }

   public TrustStoreConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(RELATIVE_TO).set(relativeTo);
      return this;
   }

   public void build() {
      if (trustManagerFactory == null) {
         String fileName = ParseUtils.resolvePath(attributes.attribute(PATH).get(),
               attributes.attribute(RELATIVE_TO).get());
         String provider = attributes.attribute(PROVIDER).get();
         char[] password = attributes.attribute(PASSWORD).get();
         try (FileInputStream is = new FileInputStream(fileName)) {
            trustStore = KeyStoreUtil.loadKeyStore(INSTALLED_PROVIDERS, provider, is, fileName, password);
            trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
               if (trustManager instanceof X509TrustManager) {
                  realmBuilder.sslContextBuilder().setTrustManager((X509TrustManager) trustManager);
                  return;
               }
            }
            throw Server.log.noDefaultTrustManager();
         } catch (Exception e) {
            throw new CacheConfigurationException(e);
         }
      }
   }

   public KeyStore trustStore() {
      build();
      return trustStore;
   }

   @Override
   public void validate() {
   }

   @Override
   public TrustStoreConfiguration create() {
      return new TrustStoreConfiguration(attributes.protect());
   }

   @Override
   public TrustStoreConfigurationBuilder read(TrustStoreConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}

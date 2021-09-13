package org.infinispan.server.configuration.security;

import static org.wildfly.security.provider.util.ProviderUtil.INSTALLED_PROVIDERS;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.function.Supplier;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.keystore.KeyStoreUtil;
import org.wildfly.security.ssl.SSLContextBuilder;

/**
 * @since 12.1
 */
public class TrustStoreConfiguration extends ConfigurationElement<TrustStoreConfiguration> {
   static final AttributeDefinition<Supplier<char[]>> PASSWORD = AttributeDefinition.builder(Attribute.PASSWORD, null, (Class<Supplier<char[]>>) (Class<?>) Supplier.class).serializer(AttributeSerializer.SECRET).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).autoPersist(false).build();
   static final AttributeDefinition<String> PROVIDER = AttributeDefinition.builder(Attribute.PROVIDER, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TrustStoreConfiguration.class, PASSWORD, PATH, RELATIVE_TO, PROVIDER);
   }

   TrustStoreConfiguration(AttributeSet attributes) {
      super(Element.TRUSTSTORE, attributes);
   }

   KeyStore trustStore(Properties properties) {
      String fileName = ParseUtils.resolvePath(attributes.attribute(PATH).get(),
            properties.getProperty(attributes.attribute(RELATIVE_TO).get()));
      if (fileName == null) {
         return null;
      } else {
         String provider = attributes.attribute(PROVIDER).get();
         char[] password = attributes.attribute(PASSWORD).get().get();
         try (FileInputStream is = new FileInputStream(fileName)) {
            return KeyStoreUtil.loadKeyStore(INSTALLED_PROVIDERS, provider, is, fileName, password);
         } catch (IOException | KeyStoreException e) {
            throw new CacheConfigurationException(e);
         }
      }
   }

   public void build(SSLContextBuilder builder, Properties properties) {
      try {
         KeyStore trustStore = trustStore(properties);
         TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
         trustManagerFactory.init(trustStore);
         for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
            if (trustManager instanceof X509TrustManager) {
               builder.setTrustManager((X509TrustManager) trustManager);
               return;
            }
         }
         throw Server.log.noDefaultTrustManager();
      } catch (NoSuchAlgorithmException | KeyStoreException e) {
         throw new CacheConfigurationException(e);
      }
   }
}

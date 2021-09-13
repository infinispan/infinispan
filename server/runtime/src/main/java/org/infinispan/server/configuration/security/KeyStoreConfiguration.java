package org.infinispan.server.configuration.security;

import static org.wildfly.security.provider.util.ProviderUtil.INSTALLED_PROVIDERS;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Properties;
import java.util.function.Supplier;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.KeyStoreUtils;
import org.wildfly.security.keystore.AliasFilter;
import org.wildfly.security.keystore.FilteringKeyStore;
import org.wildfly.security.keystore.KeyStoreUtil;
import org.wildfly.security.ssl.SSLContextBuilder;

/**
 * @since 10.0
 */
public class KeyStoreConfiguration extends ConfigurationElement<KeyStoreConfiguration> {
   static final AttributeDefinition<String> ALIAS = AttributeDefinition.builder(Attribute.ALIAS, null, String.class).build();
   static final AttributeDefinition<String> GENERATE_SELF_SIGNED_CERTIFICATE_HOST = AttributeDefinition.builder(Attribute.GENERATE_SELF_SIGNED_CERTIFICATE_HOST, null, String.class).build();
   static final AttributeDefinition<Supplier<char[]>> KEY_PASSWORD = AttributeDefinition.builder(Attribute.KEY_PASSWORD, null, (Class<Supplier<char[]>>) (Class<?>) Supplier.class)
         .serializer(AttributeSerializer.SECRET).build();
   static final AttributeDefinition<Supplier<char[]>> KEYSTORE_PASSWORD = AttributeDefinition.builder(Attribute.KEYSTORE_PASSWORD, null, (Class<Supplier<char[]>>) (Class<?>) Supplier.class)
         .serializer(AttributeSerializer.SECRET).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).autoPersist(false).build();
   static final AttributeDefinition<String> PROVIDER = AttributeDefinition.builder(Attribute.PROVIDER, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(KeyStoreConfiguration.class, ALIAS, GENERATE_SELF_SIGNED_CERTIFICATE_HOST, KEY_PASSWORD, KEYSTORE_PASSWORD, PATH, RELATIVE_TO, PROVIDER);
   }

   KeyStoreConfiguration(AttributeSet attributes) {
      super(Element.KEYSTORE, attributes);
   }

   public void build(SSLContextBuilder builder, Properties properties) {
      try {
         String keyStoreFileName = ParseUtils.resolvePath(attributes.attribute(PATH).get(),
               properties.getProperty(attributes.attribute(RELATIVE_TO).get()));
         String generateSelfSignedHost = attributes.attribute(GENERATE_SELF_SIGNED_CERTIFICATE_HOST).get();
         String provider = attributes.attribute(PROVIDER).get();
         char[] keyStorePassword = attributes.attribute(KEYSTORE_PASSWORD).get().get();
         char[] keyPassword = attributes.attribute(KEY_PASSWORD).isNull() ? null : attributes.attribute(KEY_PASSWORD).get().get();
         String keyAlias = attributes.attribute(ALIAS).get();
         if (!new File(keyStoreFileName).exists() && generateSelfSignedHost != null) {
            KeyStoreUtils.generateSelfSignedCertificate(keyStoreFileName, provider, keyStorePassword,
                  keyPassword, keyAlias, generateSelfSignedHost);
         }
         KeyStore keyStore = KeyStoreUtil.loadKeyStore(INSTALLED_PROVIDERS, provider,
               new FileInputStream(keyStoreFileName), keyStoreFileName, keyStorePassword);
         if (keyAlias != null) {
            keyStore = FilteringKeyStore.filteringKeyStore(keyStore, AliasFilter.fromString(keyAlias));
         }
         KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
         keyManagerFactory.init(keyStore, keyPassword != null ? keyPassword : keyStorePassword);
         for (KeyManager keyManager : keyManagerFactory.getKeyManagers()) {
            if (keyManager instanceof X509ExtendedKeyManager) {
               builder.setKeyManager((X509ExtendedKeyManager) keyManager);
               if (provider != null) {
                  builder.setProviderName(provider);
               }
               return;
            }
         }
         throw Server.log.noDefaultKeyManager();
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }
   }
}

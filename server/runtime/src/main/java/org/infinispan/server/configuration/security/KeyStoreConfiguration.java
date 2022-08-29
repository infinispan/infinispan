package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.CredentialStoresConfiguration.resolvePassword;
import static org.infinispan.server.security.KeyStoreUtils.buildFilelessKeyStore;
import static org.wildfly.security.provider.util.ProviderUtil.findProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Provider;
import java.util.EnumSet;
import java.util.Properties;
import java.util.function.Supplier;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.configuration.ServerConfigurationSerializer;
import org.infinispan.server.security.KeyStoreUtils;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.credential.source.CredentialSource;
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
   @Deprecated
   static final AttributeDefinition<Supplier<CredentialSource>> KEY_PASSWORD = AttributeDefinition.builder(Attribute.KEY_PASSWORD, null, (Class<Supplier<CredentialSource>>) (Class<?>) Supplier.class)
         .serializer(ServerConfigurationSerializer.CREDENTIAL).build();
   static final AttributeDefinition<Supplier<CredentialSource>> KEYSTORE_PASSWORD = AttributeDefinition.builder(Attribute.PASSWORD, null, (Class<Supplier<CredentialSource>>) (Class<?>) Supplier.class)
         .serializer(ServerConfigurationSerializer.CREDENTIAL).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).autoPersist(false).build();
   static final AttributeDefinition<String> PROVIDER = AttributeDefinition.builder(Attribute.PROVIDER, null, String.class).build();
   static final AttributeDefinition<String> TYPE = AttributeDefinition.builder(Attribute.TYPE, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(KeyStoreConfiguration.class, ALIAS, GENERATE_SELF_SIGNED_CERTIFICATE_HOST, PATH, RELATIVE_TO, PROVIDER, KEY_PASSWORD, KEYSTORE_PASSWORD, TYPE);
   }

   KeyStoreConfiguration(AttributeSet attributes) {
      super(Element.KEYSTORE, attributes);
   }

   public void build(SSLContextBuilder builder, Properties properties, EnumSet<ServerSecurityRealm.Feature> features) {
      if (attributes.isModified()) {
         Provider[] providers = SecurityActions.discoverSecurityProviders(Thread.currentThread().getContextClassLoader());
         try {
            final KeyStore keyStore;
            String providerName = attributes.attribute(PROVIDER).get();
            String type = attributes.attribute(TYPE).get();
            if (attributes.attribute(PATH).isNull()) {
               keyStore = buildFilelessKeyStore(providers, providerName, type);
            } else {
               keyStore = buildKeyStore(providers, properties);
            }
            String algorithm = KeyManagerFactory.getDefaultAlgorithm();
            Provider provider = findProvider(providers, providerName, KeyManagerFactory.class, algorithm);
            KeyManagerFactory keyManagerFactory = provider != null ? KeyManagerFactory.getInstance(algorithm, provider) : KeyManagerFactory.getInstance(algorithm);
            char[] keyStorePassword = resolvePassword(attributes.attribute(KEYSTORE_PASSWORD));
            char[] keyPassword = resolvePassword(attributes.attribute(KEY_PASSWORD));
            keyManagerFactory.init(keyStore, keyPassword != null ? keyPassword : keyStorePassword);
            for (KeyManager keyManager : keyManagerFactory.getKeyManagers()) {
               if (keyManager instanceof X509ExtendedKeyManager) {
                  builder.setKeyManager((X509ExtendedKeyManager) keyManager);
                  features.add(ServerSecurityRealm.Feature.ENCRYPT);
                  return;
               }
            }
            throw Server.log.noDefaultKeyManager();
         } catch (Exception e) {
            throw new CacheConfigurationException(e);
         }
      }
   }

   private KeyStore buildKeyStore(Provider[] providers, Properties properties) throws GeneralSecurityException, IOException {
      String keyStoreFileName = ParseUtils.resolvePath(attributes.attribute(PATH).get(),
            properties.getProperty(attributes.attribute(RELATIVE_TO).get()));
      String generateSelfSignedHost = attributes.attribute(GENERATE_SELF_SIGNED_CERTIFICATE_HOST).get();
      String provider = attributes.attribute(PROVIDER).get();
      char[] keyStorePassword = resolvePassword(attributes.attribute(KEYSTORE_PASSWORD));
      char[] keyPassword = resolvePassword(attributes.attribute(KEY_PASSWORD));
      String keyAlias = attributes.attribute(ALIAS).get();
      if (!new File(keyStoreFileName).exists() && generateSelfSignedHost != null) {
         KeyStoreUtils.generateSelfSignedCertificate(keyStoreFileName, provider, keyStorePassword,
               keyPassword, keyAlias, generateSelfSignedHost);
      }
      KeyStore keyStore = KeyStoreUtil.loadKeyStore(() -> providers, provider, new FileInputStream(keyStoreFileName), keyStoreFileName, keyStorePassword);
      if (keyAlias != null) {
         if (!keyStore.containsAlias(keyAlias)) {
            throw Server.log.aliasNotInKeystore(keyAlias, keyStoreFileName);
         }
         keyStore = FilteringKeyStore.filteringKeyStore(keyStore, AliasFilter.fromString(keyAlias));
      }
      return keyStore;
   }
}

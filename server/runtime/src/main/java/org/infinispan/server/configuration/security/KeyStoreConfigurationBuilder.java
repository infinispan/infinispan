package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.KeyStoreConfiguration.ALIAS;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.GENERATE_SELF_SIGNED_CERTIFICATE_HOST;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.KEYSTORE_PASSWORD;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.KEY_PASSWORD;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.PATH;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.PROVIDER;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.RELATIVE_TO;
import static org.wildfly.security.provider.util.ProviderUtil.INSTALLED_PROVIDERS;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Properties;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.security.KeyStoreUtils;
import org.wildfly.security.keystore.AliasFilter;
import org.wildfly.security.keystore.FilteringKeyStore;
import org.wildfly.security.keystore.KeyStoreUtil;

/**
 * @since 10.0
 */
public class KeyStoreConfigurationBuilder implements Builder<KeyStoreConfiguration> {
   private final AttributeSet attributes;
   private final RealmConfigurationBuilder realmBuilder;
   private KeyManagerFactory keyManagerFactory;

   KeyStoreConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
      this.attributes = KeyStoreConfiguration.attributeDefinitionSet();
   }

   public KeyStoreConfigurationBuilder alias(String alias) {
      attributes.attribute(ALIAS).set(alias);
      return this;
   }

   public KeyStoreConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      attributes.attribute(KEYSTORE_PASSWORD).set(keyStorePassword);
      return this;
   }

   public KeyStoreConfigurationBuilder generateSelfSignedCertificateHost(String certificateHost) {
      attributes.attribute(GENERATE_SELF_SIGNED_CERTIFICATE_HOST).set(certificateHost);
      return this;
   }

   public KeyStoreConfigurationBuilder keyPassword(char[] keyPassword) {
      attributes.attribute(KEY_PASSWORD).set(keyPassword);
      return this;
   }

   public KeyStoreConfigurationBuilder path(String path) {
      attributes.attribute(PATH).set(path);
      return this;
   }

   public KeyStoreConfigurationBuilder provider(String value) {
      attributes.attribute(PROVIDER).set(value);
      return this;
   }

   public KeyStoreConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(RELATIVE_TO).set(relativeTo);
      return this;
   }

   public void build(Properties properties) {
      if (keyManagerFactory == null) {
         try {
            String keyStoreFileName = ParseUtils.resolvePath(attributes.attribute(PATH).get(),
                  properties.getProperty(attributes.attribute(RELATIVE_TO).get()));
            String generateSelfSignedHost = attributes.attribute(GENERATE_SELF_SIGNED_CERTIFICATE_HOST).get();
            String keyStoreProvider = attributes.attribute(PROVIDER).get();
            char[] keyStorePassword = attributes.attribute(KEYSTORE_PASSWORD).get();
            char[] keyPassword = attributes.attribute(KEY_PASSWORD).get();
            String keyAlias = attributes.attribute(ALIAS).get();
            if (!new File(keyStoreFileName).exists() && generateSelfSignedHost != null) {
               KeyStoreUtils.generateSelfSignedCertificate(keyStoreFileName, keyStoreProvider, keyStorePassword,
                     keyPassword, keyAlias, generateSelfSignedHost);
            }
            KeyStore keyStore = KeyStoreUtil.loadKeyStore(INSTALLED_PROVIDERS, keyStoreProvider,
                  new FileInputStream(keyStoreFileName), keyStoreFileName, keyStorePassword);
            if (keyAlias != null) {
               keyStore = FilteringKeyStore.filteringKeyStore(keyStore, AliasFilter.fromString(keyAlias));
            }
            keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, keyPassword != null ? keyPassword : keyStorePassword);
            for (KeyManager keyManager : keyManagerFactory.getKeyManagers()) {
               if (keyManager instanceof X509ExtendedKeyManager) {
                  realmBuilder.sslContextBuilder().setKeyManager((X509ExtendedKeyManager) keyManager);
                  return;
               }
            }
            throw Server.log.noDefaultKeyManager();
         } catch (Exception e) {
            throw new CacheConfigurationException(e);
         }
      }
   }

   @Override
   public void validate() {
   }

   @Override
   public KeyStoreConfiguration create() {
      return new KeyStoreConfiguration(attributes.protect());
   }

   @Override
   public KeyStoreConfigurationBuilder read(KeyStoreConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}

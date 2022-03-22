package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.KeyStoreConfiguration.ALIAS;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.GENERATE_SELF_SIGNED_CERTIFICATE_HOST;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.KEYSTORE_PASSWORD;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.KEY_PASSWORD;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.PATH;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.PROVIDER;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.RELATIVE_TO;
import static org.infinispan.server.configuration.security.KeyStoreConfiguration.TYPE;

import java.util.function.Supplier;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.Server;
import org.infinispan.server.security.PasswordCredentialSource;
import org.wildfly.security.credential.source.CredentialSource;

/**
 * @since 10.0
 */
public class KeyStoreConfigurationBuilder implements Builder<KeyStoreConfiguration> {
   private final AttributeSet attributes;
   private final RealmConfigurationBuilder realmBuilder;

   KeyStoreConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
      this.attributes = KeyStoreConfiguration.attributeDefinitionSet();
   }

   public KeyStoreConfigurationBuilder alias(String alias) {
      attributes.attribute(ALIAS).set(alias);
      return this;
   }

   public KeyStoreConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      attributes.attribute(KEYSTORE_PASSWORD).set(new PasswordCredentialSource(keyStorePassword));
      return this;
   }

   public KeyStoreConfigurationBuilder keyStorePassword(Supplier<CredentialSource> keyStorePassword) {
      attributes.attribute(KEYSTORE_PASSWORD).set(keyStorePassword);
      return this;
   }

   public KeyStoreConfigurationBuilder generateSelfSignedCertificateHost(String certificateHost) {
      attributes.attribute(GENERATE_SELF_SIGNED_CERTIFICATE_HOST).set(certificateHost);
      return this;
   }

   @Deprecated
   public KeyStoreConfigurationBuilder keyPassword(char[] keyPassword) {
      attributes.attribute(KEY_PASSWORD).set(new PasswordCredentialSource(keyPassword));
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

   public KeyStoreConfigurationBuilder type(String value) {
      attributes.attribute(TYPE).set(value);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(PATH).isNull() && attributes.attribute(TYPE).isNull()) {
         throw Server.log.filelessKeyStoreRequiresType();
      }
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

package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.KeyStoreConfiguration.KEYSTORE_CERTIFICATE_PASSWORD;
import static org.infinispan.persistence.remote.configuration.KeyStoreConfiguration.KEYSTORE_FILENAME;
import static org.infinispan.persistence.remote.configuration.KeyStoreConfiguration.KEYSTORE_PASSWORD;
import static org.infinispan.persistence.remote.configuration.KeyStoreConfiguration.KEYSTORE_TYPE;
import static org.infinispan.persistence.remote.configuration.KeyStoreConfiguration.KEY_ALIAS;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class KeyStoreConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<KeyStoreConfiguration> {
   private static final Log log = LogFactory.getLog(KeyStoreConfigurationBuilder.class);

   protected KeyStoreConfigurationBuilder(SecurityConfigurationBuilder builder) {
      super(builder, KeyStoreConfiguration.attributeDefinitionSet());
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public KeyStoreConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      this.attributes.attribute(KEYSTORE_FILENAME).set(keyStoreFileName);
      return this;
   }

   public KeyStoreConfigurationBuilder keyStoreType(String keyStoreType) {
      this.attributes.attribute(KEYSTORE_TYPE).set(keyStoreType);
      return this;
   }

   public KeyStoreConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      this.attributes.attribute(KEYSTORE_PASSWORD).set(new String(keyStorePassword));
      return this;
   }

   public KeyStoreConfigurationBuilder keyStoreCertificatePassword(char[] keyStoreCertificatePassword) {
      this.attributes.attribute(KEYSTORE_CERTIFICATE_PASSWORD).set(new String(keyStoreCertificatePassword));
      return this;
   }

   public KeyStoreConfigurationBuilder keyAlias(String keyAlias) {
      this.attributes.attribute(KEY_ALIAS).set(keyAlias);
      return this;
   }

   @Override
   public KeyStoreConfiguration create() {
      return new KeyStoreConfiguration(attributes.protect());
   }

   @Override
   public KeyStoreConfigurationBuilder read(KeyStoreConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }
}

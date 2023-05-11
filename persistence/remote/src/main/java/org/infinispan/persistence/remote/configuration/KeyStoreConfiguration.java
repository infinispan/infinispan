package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.Util;

/**
 * @since 10.0
 */
public class KeyStoreConfiguration extends ConfigurationElement<KeyStoreConfiguration> {

   static final AttributeDefinition<String> KEYSTORE_FILENAME = AttributeDefinition.builder(Attribute.FILENAME, null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> KEYSTORE_TYPE = AttributeDefinition.builder(Attribute.TYPE, "JKS", String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> KEYSTORE_PASSWORD = AttributeDefinition.builder(Attribute.PASSWORD, null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> KEYSTORE_CERTIFICATE_PASSWORD = AttributeDefinition.builder(Attribute.CERTIFICATE_PASSWORD, null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> KEY_ALIAS = AttributeDefinition.builder(Attribute.KEY_ALIAS, null, String.class).immutable().autoPersist(false).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SslConfiguration.class, KEYSTORE_FILENAME, KEYSTORE_TYPE, KEYSTORE_PASSWORD, KEYSTORE_CERTIFICATE_PASSWORD, KEY_ALIAS);
   }

   KeyStoreConfiguration(AttributeSet attributes) {
      super(Element.KEYSTORE, attributes);
   }

   public String keyStoreFileName() {
      return attributes.attribute(KEYSTORE_FILENAME).get();
   }

   public String keyStoreType() {
      return attributes.attribute(KEYSTORE_TYPE).get();
   }

   public char[] keyStorePassword() {
      return Util.toCharArray(attributes.attribute(KEYSTORE_PASSWORD).get());
   }

   public char[] keyStoreCertificatePassword() {
      return Util.toCharArray(attributes.attribute(KEYSTORE_CERTIFICATE_PASSWORD).get());
   }

   public String keyAlias() {
      return attributes.attribute(KEY_ALIAS).get();
   }
}

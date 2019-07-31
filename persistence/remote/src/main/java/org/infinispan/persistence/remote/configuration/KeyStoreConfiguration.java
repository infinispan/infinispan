package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.Element.KEYSTORE;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.util.Util;

/**
 * @since 10.0
 */
public class KeyStoreConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<String> KEYSTORE_FILENAME = AttributeDefinition.builder("keyStoreFilename", null, String.class).xmlName(org.infinispan.persistence.remote.configuration.Attribute.FILENAME.getLocalName()).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> KEYSTORE_TYPE = AttributeDefinition.builder("keyStoreType", "JKS", String.class).immutable().autoPersist(false).xmlName(org.infinispan.persistence.remote.configuration.Attribute.TYPE.getLocalName()).build();
   static final AttributeDefinition<String> KEYSTORE_PASSWORD = AttributeDefinition.builder("keyStorePassword", null, String.class).xmlName(org.infinispan.persistence.remote.configuration.Attribute.PASSWORD.getLocalName()).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> KEYSTORE_CERTIFICATE_PASSWORD = AttributeDefinition.builder("keyStoreCertificatePassword", null, String.class).immutable().xmlName(org.infinispan.persistence.remote.configuration.Attribute.CERTIFICATE_PASSWORD.getLocalName()).autoPersist(false).build();
   static final AttributeDefinition<String> KEY_ALIAS = AttributeDefinition.builder("keyAlias", null, String.class).immutable().autoPersist(false).build();

   static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(KEYSTORE.getLocalName());
   private final AttributeSet attributes;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SslConfiguration.class, KEYSTORE_FILENAME, KEYSTORE_TYPE, KEYSTORE_PASSWORD, KEYSTORE_CERTIFICATE_PASSWORD, KEY_ALIAS);
   }

   KeyStoreConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public AttributeSet attributes() {
      return attributes;
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

   @Override
   public String toString() {
      return attributes.toString();
   }


   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      KeyStoreConfiguration that = (KeyStoreConfiguration) o;

      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }
}

package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.Element.TRUSTSTORE;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.util.Util;

public class TrustStoreConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<String> TRUSTSTORE_FILENAME = AttributeDefinition.builder("trustStoreFilename", null, String.class).immutable().autoPersist(false).xmlName(org.infinispan.persistence.remote.configuration.Attribute.FILENAME.getLocalName()).build();
   static final AttributeDefinition<String> TRUSTSTORE_TYPE = AttributeDefinition.builder("trustStoreType", "JKS", String.class).immutable().autoPersist(false).xmlName(org.infinispan.persistence.remote.configuration.Attribute.TYPE.getLocalName()).build();
   static final AttributeDefinition<String> TRUSTSTORE_PASSWORD = AttributeDefinition.builder("trustStorePassword", null, String.class).immutable().autoPersist(false).xmlName(org.infinispan.persistence.remote.configuration.Attribute.PASSWORD.getLocalName()).build();

   static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(TRUSTSTORE.getLocalName());
   private final AttributeSet attributes;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TrustStoreConfiguration.class, TRUSTSTORE_FILENAME, TRUSTSTORE_TYPE, TRUSTSTORE_PASSWORD);
   }

   TrustStoreConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String trustStoreFileName() {
      return attributes.attribute(TRUSTSTORE_FILENAME).get();
   }

   public String trustStoreType() {
      return attributes.attribute(TRUSTSTORE_TYPE).get();
   }

   public char[] trustStorePassword() {
      return Util.toCharArray(attributes.attribute(TRUSTSTORE_PASSWORD).get());
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TrustStoreConfiguration that = (TrustStoreConfiguration) o;

      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }
}

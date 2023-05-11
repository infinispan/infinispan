package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.Util;

public class TrustStoreConfiguration extends ConfigurationElement<TrustStoreConfiguration> {

   static final AttributeDefinition<String> TRUSTSTORE_FILENAME = AttributeDefinition.builder(Attribute.FILENAME, null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> TRUSTSTORE_TYPE = AttributeDefinition.builder(Attribute.TYPE, "JKS", String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> TRUSTSTORE_PASSWORD = AttributeDefinition.builder(Attribute.PASSWORD, null, String.class).immutable().autoPersist(false).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TrustStoreConfiguration.class, TRUSTSTORE_FILENAME, TRUSTSTORE_TYPE, TRUSTSTORE_PASSWORD);
   }

   TrustStoreConfiguration(AttributeSet attributes) {
      super(Element.TRUSTSTORE, attributes);
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
}

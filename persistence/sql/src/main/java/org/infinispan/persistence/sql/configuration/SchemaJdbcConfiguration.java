package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.persistence.jdbc.common.configuration.Attribute;
import org.infinispan.persistence.jdbc.common.configuration.Element;

public class SchemaJdbcConfiguration extends ConfigurationElement<SchemaJdbcConfiguration> {
   public static final AttributeDefinition<String> MESSAGE_NAME = AttributeDefinition.builder(Attribute.MESSAGE_NAME, null, String.class).immutable().build();
   public static final AttributeDefinition<String> KEY_MESSAGE_NAME = AttributeDefinition.builder(Attribute.KEY_MESSAGE_NAME, null, String.class).immutable().build();
   public static final AttributeDefinition<String> PACKAGE = AttributeDefinition.builder(Attribute.PACKAGE, null, String.class).immutable().build();
   public static final AttributeDefinition<Boolean> EMBEDDED_KEY = AttributeDefinition.builder(Attribute.EMBEDDED_KEY, Boolean.FALSE).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SchemaJdbcConfiguration.class, MESSAGE_NAME, KEY_MESSAGE_NAME, PACKAGE, EMBEDDED_KEY);
   }

   SchemaJdbcConfiguration(AttributeSet attributes) {
      super(Element.SCHEMA, attributes);
   }

   public String messageName() {
      return attributes.attribute(MESSAGE_NAME).get();
   }

   public String keyMessageName() {
      return attributes.attribute(KEY_MESSAGE_NAME).get();
   }

   public String packageName() {
      return attributes.attribute(PACKAGE).get();
   }

   public boolean embeddedKey() {
      return attributes.attribute(EMBEDDED_KEY).get();
   }
}

package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.persistence.jdbc.common.configuration.Attribute;
import org.infinispan.persistence.jdbc.common.configuration.Element;

public class QueriesJdbcConfiguration extends ConfigurationElement<QueriesJdbcConfiguration> {
   public static final AttributeDefinition<String> SELECT = AttributeDefinition.builder(Attribute.SELECT_SINGLE, null, String.class).immutable().build();
   public static final AttributeDefinition<String> SELECT_ALL = AttributeDefinition.builder(Attribute.SELECT_ALL, null, String.class).immutable().build();
   public static final AttributeDefinition<String> DELETE = AttributeDefinition.builder(Attribute.DELETE_SINGLE, null, String.class).immutable().build();
   public static final AttributeDefinition<String> DELETE_ALL = AttributeDefinition.builder(Attribute.DELETE_ALL, null, String.class).immutable().build();
   public static final AttributeDefinition<String> UPSERT = AttributeDefinition.builder(Attribute.UPSERT, null, String.class).immutable().build();
   public static final AttributeDefinition<String> SIZE = AttributeDefinition.builder(Attribute.SIZE, null, String.class).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(QueriesJdbcConfiguration.class, SELECT, SELECT_ALL, DELETE, DELETE_ALL, UPSERT, SIZE);
   }

   QueriesJdbcConfiguration(AttributeSet attributes) {
      super(Element.QUERIES, attributes);
   }

   public String select() {
      return attributes.attribute(SELECT).get();
   }

   public String selectAll() {
      return attributes.attribute(SELECT_ALL).get();
   }

   public String delete() {
      return attributes.attribute(DELETE).get();
   }

   public String deleteAll() {
      return attributes.attribute(DELETE_ALL).get();
   }

   public String upsert() {
      return attributes.attribute(UPSERT).get();
   }

   public String size() {
      return attributes.attribute(SIZE).get();
   }
}

package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.jdbc.common.configuration.Attribute;

public class QueriesJdbcConfiguration {
   public static final AttributeDefinition<String> SELECT = AttributeDefinition.builder(Attribute.SELECT_SINGLE, null, String.class).immutable().build();
   public static final AttributeDefinition<String> SELECT_ALL = AttributeDefinition.builder(Attribute.SELECT_ALL, null, String.class).immutable().build();
   public static final AttributeDefinition<String> DELETE = AttributeDefinition.builder(Attribute.DELETE_SINGLE, null, String.class).immutable().build();
   public static final AttributeDefinition<String> DELETE_ALL = AttributeDefinition.builder(Attribute.DELETE_ALL, null, String.class).immutable().build();
   public static final AttributeDefinition<String> UPSERT = AttributeDefinition.builder(Attribute.UPSERT, null, String.class).immutable().build();
   public static final AttributeDefinition<String> SIZE = AttributeDefinition.builder(Attribute.SIZE, null, String.class).immutable().build();

   private final AttributeSet attributes;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(QueriesJdbcConfiguration.class, SELECT, SELECT_ALL, DELETE, DELETE_ALL, UPSERT, SIZE);
   }

   QueriesJdbcConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   AttributeSet attributes() {
      return attributes;
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

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      QueriesJdbcConfiguration that = (QueriesJdbcConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "QueriesConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}

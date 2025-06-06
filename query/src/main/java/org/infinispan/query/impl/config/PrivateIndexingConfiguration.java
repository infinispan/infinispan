package org.infinispan.query.impl.config;

import java.util.Objects;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @api.private
 */
public class PrivateIndexingConfiguration {

   public static final AttributeDefinition<Integer> REBATCH_REQUESTS_SIZE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.REBATCH_REQUESTS_SIZE, 10_000).immutable().build();

   final AttributeSet attributes;

   public PrivateIndexingConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(PrivateIndexingConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(),
            REBATCH_REQUESTS_SIZE);
   }

   public int rebatchRequestsSize() {
      return attributes.attribute(REBATCH_REQUESTS_SIZE).get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PrivateIndexingConfiguration that = (PrivateIndexingConfiguration) o;
      return Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(attributes);
   }

   @Override
   public String toString() {
      return "PrivateIndexingConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}

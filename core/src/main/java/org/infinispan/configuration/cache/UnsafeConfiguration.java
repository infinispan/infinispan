package org.infinispan.configuration.cache;

import java.util.Map;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 *
 * Controls certain tuning parameters that may break some of Infinispan's public API contracts in
 * exchange for better performance in some cases.
 * <p />
 * Use with care, only after thoroughly reading and understanding the documentation about a specific
 * feature.
 * <p />
 *
 * @see UnsafeConfigurationBuilder
 */
public class UnsafeConfiguration {
   public static final AttributeDefinition<Boolean> UNRELIABLE_RETURN_VALUES = AttributeDefinition.builder("unrealiableReturnValues", false).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(UnsafeConfiguration.class, UNRELIABLE_RETURN_VALUES);
   }
   private final Attribute<Boolean> unreliableReturnValues;
   private final AttributeSet attributes;

   UnsafeConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      unreliableReturnValues = attributes.attribute(UNRELIABLE_RETURN_VALUES);
   }

   /**
    * Specifies whether Infinispan is allowed to disregard the {@link Map} contract when providing
    * return values for {@link org.infinispan.Cache#put(Object, Object)} and
    * {@link org.infinispan.Cache#remove(Object)} methods.
    */
   public boolean unreliableReturnValues() {
      return unreliableReturnValues.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return attributes.toString();
   }

   @Override
   public boolean equals(Object o) {
      UnsafeConfiguration other = (UnsafeConfiguration) o;
      return attributes.equals(other.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

}

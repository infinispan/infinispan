package org.infinispan.commons.configuration;

import static org.infinispan.commons.util.Immutables.immutableTypedProperties;

import org.infinispan.commons.util.TypedProperties;

public abstract class AbstractTypedPropertiesConfiguration {

   private final TypedProperties properties;

   protected AbstractTypedPropertiesConfiguration(TypedProperties properties) {
      this.properties = immutableTypedProperties(properties);
   }

   public TypedProperties properties() {
      return properties;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AbstractTypedPropertiesConfiguration that = (AbstractTypedPropertiesConfiguration) o;

      if (properties != null ? !properties.equals(that.properties) : that.properties != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      return properties != null ? properties.hashCode() : 0;
   }

}
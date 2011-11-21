package org.infinispan.configuration;

import static org.infinispan.util.Immutables.immutableTypedPropreties;

import org.infinispan.util.TypedProperties;

public abstract class AbstractTypedPropertiesConfiguration {

   private final TypedProperties properties;
   
   protected AbstractTypedPropertiesConfiguration(TypedProperties properties) {
      this.properties = immutableTypedPropreties(properties);
   }

   public TypedProperties properties() {
      return properties;
   }
}
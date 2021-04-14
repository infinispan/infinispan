package org.infinispan.configuration.global;

import java.util.Objects;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class ShutdownConfiguration {
   static final AttributeDefinition<ShutdownHookBehavior> HOOK_BEHAVIOR = AttributeDefinition.builder("shutdownHook", ShutdownHookBehavior.DEFAULT).immutable().build();

   private final AttributeSet attributes;
   private final Attribute<ShutdownHookBehavior> hookBehavior;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ShutdownConfiguration.class, HOOK_BEHAVIOR);
   }

   ShutdownConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      hookBehavior = attributes.attribute(HOOK_BEHAVIOR);
   }

   public ShutdownHookBehavior hookBehavior() {
      return hookBehavior.get();
   }

   @Override
   public String toString() {
      return "ShutdownConfiguration{" +
            "attributes=" + attributes +
            '}';
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ShutdownConfiguration that = (ShutdownConfiguration) o;

      return Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }
}

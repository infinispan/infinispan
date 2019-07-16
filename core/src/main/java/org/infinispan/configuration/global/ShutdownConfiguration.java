package org.infinispan.configuration.global;

import java.util.Objects;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;

public class ShutdownConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<ShutdownHookBehavior> HOOK_BEHAVIOR = AttributeDefinition.builder("shutdownHook", ShutdownHookBehavior.DEFAULT).immutable().build();

   private final AttributeSet attributes;
   private final Attribute<ShutdownHookBehavior> hookBehavior;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ShutdownConfiguration.class, HOOK_BEHAVIOR);
   }

   static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SHUTDOWN.getLocalName(), false);

   ShutdownConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      hookBehavior = attributes.attribute(HOOK_BEHAVIOR);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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

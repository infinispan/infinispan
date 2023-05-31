package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.ShutdownConfiguration.HOOK_BEHAVIOR;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class ShutdownConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<ShutdownConfiguration> {
   private final AttributeSet attributes;

   ShutdownConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = ShutdownConfiguration.attributeDefinitionSet();
   }

   public ShutdownConfigurationBuilder hookBehavior(ShutdownHookBehavior hookBehavior) {
      attributes.attribute(HOOK_BEHAVIOR).set(hookBehavior);
      return this;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public
   void validate() {
      // No-op, no validation required
   }

   @Override
   public
   ShutdownConfiguration create() {
      return new ShutdownConfiguration(attributes.protect());
   }

   @Override
   public
   ShutdownConfigurationBuilder read(ShutdownConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "ShutdownConfigurationBuilder{" +
            "attributes=" + attributes +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ShutdownConfigurationBuilder that = (ShutdownConfigurationBuilder) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}

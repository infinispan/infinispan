package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 *
 * GlobalStatePersistenceConfiguration.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class GlobalStatePersistenceConfiguration {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   public static final AttributeDefinition<String> LOCATION = AttributeDefinition.builder("location", null, String.class).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStatePersistenceConfiguration.class, ENABLED, LOCATION);
   }

   private final AttributeSet attributes;
   private final Attribute<Boolean> enabled;
   private Attribute<String> location;

   public GlobalStatePersistenceConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED);
      this.location = attributes.attribute(LOCATION);
   }

   public boolean enabled() {
      return enabled.get();
   }

   public String location() {
      return location.get();
   }

   @Override
   public String toString() {
      return "GlobalStatePersistenceConfiguration [attributes=" + attributes + "]";
   }
}

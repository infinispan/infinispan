package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * This configuration element controls whether entries are versioned. Versioning is necessary, for example, when
 * using optimistic transactions in a clustered environment, to be able to perform write-skew checks.
 */
public class VersioningConfiguration {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   public static final AttributeDefinition<VersioningScheme> SCHEME = AttributeDefinition.builder("scheme", VersioningScheme.NONE).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(VersioningConfiguration.class, ENABLED, SCHEME);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<VersioningScheme> scheme;
   private AttributeSet attributes;

   VersioningConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      enabled = attributes.attribute(ENABLED);
      scheme = attributes.attribute(SCHEME);
   }

   public boolean enabled() {
      return enabled.get();
   }

   public VersioningScheme scheme() {
      return scheme.get();
   }

   @Override
   public boolean equals(Object o) {
      VersioningConfiguration other = (VersioningConfiguration) o;
      return attributes.equals(other.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return attributes.toString();
   }

   public AttributeSet attributes() {
      return attributes;
   }

}

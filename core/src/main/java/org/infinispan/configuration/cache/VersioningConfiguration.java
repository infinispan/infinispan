package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * This configuration element controls whether entries are versioned. Versioning is necessary, for example, when
 * using optimistic transactions in a clustered environment, to be able to perform write-skew checks.
 */
public class VersioningConfiguration {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   static final AttributeDefinition<VersioningScheme> SCHEME = AttributeDefinition.builder("scheme", VersioningScheme.NONE).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(VersioningConfiguration.class, ENABLED, SCHEME);
   }

   private AttributeSet attributes;

   VersioningConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).asBoolean();
   }

   public VersioningScheme scheme() {
      return attributes.attribute(SCHEME).asObject(VersioningScheme.class);
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

   AttributeSet attributes() {
      return attributes;
   }

}

package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;

/**
 * This configuration element controls whether entries are versioned. Versioning is necessary, for example, when
 * using optimistic transactions in a clustered environment, to be able to perform write-skew checks.
 * @deprecated since 9.0. Infinispan automatically enable versioning when needed.
 */
@Deprecated
public class VersioningConfiguration implements Matchable<VersioningConfiguration> {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", true).immutable().autoPersist(false).build();
   public static final AttributeDefinition<VersioningScheme> SCHEME = AttributeDefinition.builder("scheme", VersioningScheme.SIMPLE).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(VersioningConfiguration.class, ENABLED, SCHEME);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<VersioningScheme> scheme;
   private final AttributeSet attributes;

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

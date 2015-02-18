package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Defines recovery configuration for the cache.
 *
 * @author pmuir
 *
 */
public class RecoveryConfiguration {
   public static final String DEFAULT_RECOVERY_INFO_CACHE = "__recoveryInfoCacheName__";
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   public static final AttributeDefinition<String> RECOVERY_INFO_CACHE_NAME = AttributeDefinition.builder("recoveryInfoCacheName", DEFAULT_RECOVERY_INFO_CACHE).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RecoveryConfiguration.class, ENABLED, RECOVERY_INFO_CACHE_NAME);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<String> recoveryInfoCacheName;
   private final AttributeSet attributes;

   RecoveryConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      enabled = attributes.attribute(ENABLED);
      recoveryInfoCacheName = attributes.attribute(RECOVERY_INFO_CACHE_NAME);
   }

   /**
    * Determines if recovery is enabled for the cache.
    */
   public boolean enabled() {
      return enabled.get();
   }

   /**
    * Sets the name of the cache where recovery related information is held. If not specified
    * defaults to a cache named {@link RecoveryConfiguration#DEFAULT_RECOVERY_INFO_CACHE}
    */
   public String recoveryInfoCacheName() {
      return recoveryInfoCacheName.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "RecoveryConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      RecoveryConfiguration other = (RecoveryConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

}

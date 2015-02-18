package org.infinispan.configuration.cache;

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
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   static final AttributeDefinition<String> RECOVERY_INFO_CACHE_NAME = AttributeDefinition.builder("recoveryInfoCacheName", DEFAULT_RECOVERY_INFO_CACHE).immutable().build();
   static AttributeSet attributeSet() {
      return new AttributeSet(RecoveryConfiguration.class, ENABLED, RECOVERY_INFO_CACHE_NAME);
   }
   private final AttributeSet attributes;

   RecoveryConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   /**
    * Determines if recovery is enabled for the cache.
    */
   public boolean enabled() {
      return attributes.attribute(ENABLED).asBoolean();
   }

   /**
    * Sets the name of the cache where recovery related information is held. If not specified
    * defaults to a cache named {@link RecoveryConfiguration#DEFAULT_RECOVERY_INFO_CACHE}
    */
   public String recoveryInfoCacheName() {
      return attributes.attribute(RECOVERY_INFO_CACHE_NAME).asString();
   }

   AttributeSet attributes() {
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

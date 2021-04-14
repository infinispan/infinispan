package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * Defines recovery configuration for the cache.
 *
 * @author pmuir
 *
 */
public class RecoveryConfiguration extends ConfigurationElement<RecoveryConfiguration> {
   public static final String DEFAULT_RECOVERY_INFO_CACHE = "__recoveryInfoCacheName__";
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false).autoPersist(false).immutable().build();
   public static final AttributeDefinition<String> RECOVERY_INFO_CACHE_NAME = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.RECOVERY_INFO_CACHE_NAME, DEFAULT_RECOVERY_INFO_CACHE).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RecoveryConfiguration.class, ENABLED, RECOVERY_INFO_CACHE_NAME);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<String> recoveryInfoCacheName;

   RecoveryConfiguration(AttributeSet attributes) {
      super(Element.RECOVERY, attributes);
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
}

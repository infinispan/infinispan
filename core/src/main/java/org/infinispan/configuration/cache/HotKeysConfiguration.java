package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.AttributeValidator;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * Configuration for hot key tracking within a cache. When enabled, tracks the most frequently
 * accessed keys for reads and writes independently using a probabilistic top-k algorithm.
 *
 * @since 16.3
 */
public final class HotKeysConfiguration extends ConfigurationElement<HotKeysConfiguration> {

   public static final AttributeDefinition<Boolean> ENABLED =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false)
               .immutable()
               .build();

   public static final AttributeDefinition<Integer> TOP_K =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TOP_K, 100, Integer.class)
               .immutable()
               .validator(AttributeValidator.greaterThanZero(org.infinispan.configuration.parsing.Attribute.TOP_K))
               .build();

   static AttributeSet attributeDefinitions() {
      return new AttributeSet(HotKeysConfiguration.class, ENABLED, TOP_K);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<Integer> topK;

   HotKeysConfiguration(AttributeSet attributes) {
      super(Element.HOT_KEYS, attributes);
      this.enabled = attributes.attribute(ENABLED);
      this.topK = attributes.attribute(TOP_K);
   }

   /**
    * Whether hot key tracking is enabled for this cache.
    *
    * @return {@code true} if tracking is active
    */
   public boolean enabled() {
      return enabled.get();
   }

   /**
    * The number of top keys to track per operation type (reads and writes independently).
    *
    * @return the configured top-k value, defaults to 100
    */
   public int topK() {
      return topK.get();
   }
}

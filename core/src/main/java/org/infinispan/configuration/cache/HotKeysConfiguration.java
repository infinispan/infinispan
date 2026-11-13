package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.util.logging.Log;

/**
 * Configuration for hot key tracking within a cache. When enabled, tracks the most frequently
 * accessed keys for reads and writes independently using a probabilistic top-k algorithm.
 *
 * @since 16.4
 */
public final class HotKeysConfiguration extends ConfigurationElement<HotKeysConfiguration> {

   /**
    * The largest accepted {@link #TOP_K}.
    *
    * <p>
    * It is the largest value whose frequency sketch still gets its full width, so every key with more than 1/(top-k + 1)
    * of the accesses is guaranteed to be reported.
    * </p>
    */
   public static final int MAX_TOP_K = 512;

   public static final AttributeDefinition<Boolean> ENABLED =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false)
               .since(16, 4)
               .immutable()
               .build();

   public static final AttributeDefinition<Integer> TOP_K =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TOP_K, 100, Integer.class)
               .since(16, 4)
               .immutable()
               .validator(value -> {
                  if (value < 1 || value > MAX_TOP_K)
                     throw Log.CONFIG.hotKeysTopKOutOfRange(value, MAX_TOP_K);
               })
               .build();

   /**
    * Creates the attribute set of the {@code hot-keys} element with every attribute at its default.
    *
    * @return a new attribute set holding {@link #ENABLED} and {@link #TOP_K}
    */
   static AttributeSet attributeDefinitions() {
      return new AttributeSet(HotKeysConfiguration.class, ENABLED, TOP_K);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<Integer> topK;

   HotKeysConfiguration(AttributeSet attributes) {
      super(Element.HOT_KEYS.toString(), false, 16, 4, attributes);
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
    * @return the configured top-k value, between 1 and {@link #MAX_TOP_K}; defaults to 100
    */
   public int topK() {
      return topK.get();
   }
}

package org.infinispan.anchored.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.serializing.SerializedWith;

/**
 * Configuration module to transform an {@link org.infinispan.configuration.cache.CacheMode#INVALIDATION_SYNC}
 * cache into an anchored-key cache.
 *
 * <p>Anchored keys caches always write new entries to the newest member of the cache.
 * The administrator is supposed to add a new node when the current writer is close to full.</p>
 *
 * @since 11
 * @author Dan Berindei
 */
@Experimental
@SerializedWith(AnchoredKeysConfigurationSerializer.class)
@BuiltBy(AnchoredKeysConfigurationBuilder.class)
public class AnchoredKeysConfiguration {
   static final AttributeDefinition<Boolean> ENABLED =
         AttributeDefinition.builder("enabled", false).immutable().build();

   private final AttributeSet attributes;

   public AnchoredKeysConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   public static AttributeSet attributeSet() {
      return new AttributeSet(AnchoredKeysConfiguration.class, ENABLED);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   @Override
   public String toString() {
      return "AnchoredKeysConfiguration" + attributes.toString(null);
   }
}

package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.metadata.Metadata;

/**
 * This is a base class that should be used when implementing a CacheEventFilterConverter that provides default
 * implementations for the {@link org.infinispan.notifications.cachelistener.filter.CacheEventFilter#accept(Object, Object, org.infinispan.metadata.Metadata, Object, org.infinispan.metadata.Metadata, EventType)}
 * and {@link org.infinispan.filter.Converter#convert(Object, Object, org.infinispan.metadata.Metadata)}  methods so they just call the
 * {@link org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter#filterAndConvert(Object, Object, org.infinispan.metadata.Metadata, Object, org.infinispan.metadata.Metadata, EventType)}
 * method and then do the right thing.
 *
 * @author wburns
 * @since 7.0
 */
public abstract class AbstractCacheEventFilterConverter<K, V, C> implements CacheEventFilterConverter<K, V, C> {
   @Override
   public final C convert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return filterAndConvert(key, oldValue, oldMetadata, newValue, newMetadata, eventType);
   }

   @Override
   public final boolean accept(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return filterAndConvert(key, oldValue, oldMetadata, newValue, newMetadata, eventType) != null;
   }
}

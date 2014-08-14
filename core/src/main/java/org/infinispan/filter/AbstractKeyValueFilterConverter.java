package org.infinispan.filter;

import org.infinispan.metadata.Metadata;

/**
 * This is a base class that should be used when implementing a KeyValueFilterConverter that provides default
 * implementations for the {@link org.infinispan.filter.KeyValueFilter#accept(Object, Object, org.infinispan.metadata.Metadata)}
 * and {@link org.infinispan.filter.Converter#convert(Object, Object, org.infinispan.metadata.Metadata)} methods so they just call the
 * {@link org.infinispan.filter.KeyValueFilterConverter#filterAndConvert(Object, Object, org.infinispan.metadata.Metadata)}
 * method and then do the right thing.
 *
 * @author wburns
 * @since 7.0
 */
public abstract class AbstractKeyValueFilterConverter<K, V, C> implements KeyValueFilterConverter<K, V, C> {
   @Override
   public C convert(K key, V value, Metadata metadata) {
      return filterAndConvert(key, value, metadata);
   }

   @Override
   public boolean accept(K key, V value, Metadata metadata) {
      return filterAndConvert(key, value, metadata) != null;
   }
}

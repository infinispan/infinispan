package org.infinispan.iteration.impl;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.iteration.EntryIterable;

import java.util.EnumSet;

/**
 * This is an implementation that allows for creating new EntryIterable instances by supplying a new converter.
 *
 * @author wburns
 * @since 7.0
 */
public class EntryIterableFromStreamImpl<K, V> extends TrackingEntryIterableFromStream<K, V, V> implements EntryIterable<K, V> {
   public EntryIterableFromStreamImpl(KeyValueFilter<? super K, ? super V> filter, EnumSet<Flag> flags, Cache<K, V> cache) {
      super(filter, null, flags, cache);
   }

   @Override
   public <C> CloseableIterable<CacheEntry<K, C>> converter(Converter<? super K, ? super V, C> converter) {
      return new TrackingEntryIterableFromStream<>(filter, converter, null, cache);
   }
}

package org.infinispan.iteration.impl;

import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.iteration.EntryIterable;

import java.util.Map;

/**
 * This is an implementation that allows for creating new EntryIterable instances by supplying a new converter.
 *
 * @author wburns
 * @since 7.0
 */
public class EntryIterableImpl<K, V> extends TrackingEntryIterable<K, V, V> implements EntryIterable<K, V> {
   public EntryIterableImpl(EntryRetriever<K, V> entryRetriver, KeyValueFilter<? super K, ? super V> filter) {
      this(entryRetriver, filter, null);
   }

   public EntryIterableImpl(EntryRetriever<K, V> entryRetriver, KeyValueFilter<? super K, ? super V> filter,
                            Converter<? super K, ? super V, ? extends V> converter) {
      super(entryRetriver, filter, converter);
   }

   @Override
   public <C> CloseableIterable<CacheEntry<K, C>> converter(Converter<? super K, ? super V, ? extends C> converter) {
      return new TrackingEntryIterable<K, V, C>(entryRetriever, filter, converter);
   }
}

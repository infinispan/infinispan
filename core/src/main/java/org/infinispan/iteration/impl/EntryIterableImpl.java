package org.infinispan.iteration.impl;

import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.iteration.EntryIterable;

import java.util.EnumSet;
import java.util.Map;

/**
 * This is an implementation that allows for creating new EntryIterable instances by supplying a new converter.
 *
 * @author wburns
 * @since 7.0
 */
public class EntryIterableImpl<K, V> extends TrackingEntryIterable<K, V, V> implements EntryIterable<K, V> {
   public EntryIterableImpl(EntryRetriever<K, V> entryRetriver, KeyValueFilter<? super K, ? super V> filter,
                            EnumSet<Flag> flags) {
      super(entryRetriver, filter, null, flags);
   }

   @Override
   public <C> CloseableIterable<CacheEntry<K, C>> converter(Converter<? super K, ? super V, ? extends C> converter) {
      return new TrackingEntryIterable<>(entryRetriever, filter, converter, flags);
   }
}

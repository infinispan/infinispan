package org.infinispan.interceptors.compat;

import org.infinispan.CacheStream;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.compat.TypeConverter;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.AbstractDelegatingCacheStream;
import org.infinispan.stream.impl.spliterators.IteratorAsSpliterator;

import java.util.Iterator;
import java.util.Spliterator;

/**
 * Delegating stream that converts elements or CacheEntries.  Note this stream specifically doesn't specify generics as
 * it can have intermediate operations performed upon it that will change the type without changing the instance.  Also
 * depending on if CacheEntry values are returned we have to unbox those properly.
 */
public class TypeConverterStream extends AbstractDelegatingCacheStream {
   private final TypeConverter<Object, Object, Object, Object> converter;
   private final InternalEntryFactory entryFactory;

   public TypeConverterStream(CacheStream<?> stream, TypeConverter<Object, Object, Object, Object> converter,
           InternalEntryFactory entryFactory) {
      super(stream);
      this.converter = converter;
      this.entryFactory = entryFactory;
   }

   @Override
   public Iterator<Object> iterator() {
      // Note that a transformation could have been applied intermediately which means this
      // may not be a cache entry instance at all - so we have to define it as Object first
      return new IteratorMapper<>(super.iterator(), (java.lang.Object e) -> {
         if (e instanceof CacheEntry) {
            return convert((CacheEntry<?, ?>) e, converter, entryFactory);
         } else {
            // If it isn't a CacheEntry just unbox as is
            return converter.unboxValue(e);
         }
      });
   }

   @Override
   public Spliterator<Object> spliterator() {
      // We rely on our iterator to do unwrapping.
      return new IteratorAsSpliterator.Builder<>(iterator())
              .setEstimateRemaining(super.spliterator().estimateSize())
              .setCharacteristics(Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL)
              .get();
   }

   private static <K, V> CacheEntry<K, V> convert(CacheEntry<K, V> entry,
           TypeConverter<Object, Object, Object, Object> converter, InternalEntryFactory entryFactory) {
      K newKey = (K) converter.unboxKey(entry.getKey());
      V newValue = (V) converter.unboxValue(entry.getValue());
      // If either value changed then make a copy
      if (newKey != entry.getKey() || newValue != entry.getValue()) {
         return entryFactory.create(newKey, newValue, entry.getMetadata());
      }
      return entry;
   }
}

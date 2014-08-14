package org.infinispan.iteration;

import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.filter.Converter;

import java.util.Map;

/**
 * A {@link java.lang.Iterable} instance that allows the user to iterate over the entries in the cache.  This
 * also implements {@link java.io.Closeable} of which the {@link java.io.Closeable#close()} should be invoked
 * when iteration of all needed Iterables is complete.
 *
 * @author wburns
 * @since 7.0
 */
public interface EntryIterable<K, V> extends CloseableIterable<CacheEntry<K, V>> {
   /**
    * This returns a {@link org.infinispan.commons.util.CloseableIterable} that will change the type of the returned
    * value for the entry using the already provided filter in addition to the converter.
    * @param converter The converter to apply to the iterator that is produced.  Callbacks to to this converter will
    *                  never provide a key or value that is null.
    * @param <C> The type of the converted value
    * @return A CloseableIterator that will use the given converter
    */
   public <C> CloseableIterable<CacheEntry<K, C>> converter(Converter<? super K, ? super V, ? extends C> converter);
}

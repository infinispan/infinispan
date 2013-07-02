package org.infinispan.jcache;

import java.util.Iterator;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;

/**
 * An adapter to provide {@link Iterable}s over Cache Entries, those of which
 * are filtered using a {@link CacheEntryEventFilter}.
 * 
 * @author Galder Zamarreño
 * @param <K> the type of keys
 * @param <V> the type of values
 * @see Class based on the JSR-107 reference implementation (RI) of
 * {@link Iterable<CacheEntryEvent<? extends K, ? extends V>>}
 */
public class JCacheEventFilteringIterable<K, V>
      implements Iterable<CacheEntryEvent<? extends K, ? extends V>> {

    /**
     * The underlying {@link Iterable} to filter.
     */
    private Iterable<CacheEntryEvent<? extends K, ? extends V>> iterable;
    
    /**
     * The filter to apply to entries in the produced {@link Iterator}s.
     */
    private CacheEntryEventFilter<? super K, ? super V> filter;
    
    /**
     * Constructs an {@link JCacheEventFilteringIterable}.
     * 
     * @param iterable the underlying iterable to filter
     * @param filter   the filter to apply to entries in the iterable
     */
    public JCacheEventFilteringIterable(
          Iterable<CacheEntryEvent<? extends K, ? extends V>> iterable,
          CacheEntryEventFilter<? super K, ? super V> filter) {
        this.iterable = iterable;
        this.filter = filter;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<CacheEntryEvent<? extends K, ? extends V>> iterator() {
        return new JCacheEventFilteringIterator<K, V>(
              iterable.iterator(), filter);
    }

}

package org.infinispan.jcache;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;

/**
 * A adapter to {@link Iterator}s to allow filtering of {@link CacheEntryEvent}s
 * 
 * @author Galder Zamarreño
 * @param <K> the type of keys
 * @param <V> the type of value
 * @see Class based on the JSR-107 reference implementation (RI) of
 * {@link Iterator<CacheEntryEvent<? extends K, ? extends V>>}
 */
public class JCacheEventFilteringIterator<K, V>
      implements Iterator<CacheEntryEvent<? extends K, ? extends V>> {

    /**
     * The underlying iterator to filter.
     */
    private Iterator<CacheEntryEvent<? extends K, ? extends V>> iterator;
    
    /**
     * The filter to apply to Cache Entry Events in the {@link Iterator}.
     */
    private CacheEntryEventFilter<? super K, ? super V> filter;
    
    /**
     * The next available Cache Entry Event that satisfies the filter.
     * (when null we must seek to find the next event)
     */
    private CacheEntryEvent<? extends K, ? extends V> nextEntry;
    
    /**
     * Constructs an {@link JCacheEventFilteringIterator}.
     * 
     * @param iterator the underlying iterator to filter
     * @param filter   the filter to apply to entries in the iterator
     */
    public JCacheEventFilteringIterator(
          Iterator<CacheEntryEvent<? extends K, ? extends V>> iterator,
          CacheEntryEventFilter<? super K, ? super V> filter) {
        this.iterator = iterator;
        this.filter = filter;
        this.nextEntry = null;
    }
    
    /**
     * Fetches the next available, entry that satisfies the filter from 
     * the underlying iterator
     */
    private void fetch() {
        while (nextEntry == null && iterator.hasNext()) {
            CacheEntryEvent<? extends K, ? extends V> entry = iterator.next();
            
            if (filter.evaluate(entry)) {
                nextEntry = entry;
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        if (nextEntry == null) {
            fetch();
        }
        return nextEntry != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CacheEntryEvent<? extends K, ? extends V> next() {
        if (hasNext()) {
            CacheEntryEvent<? extends K, ? extends V> entry = nextEntry;
            
            //reset nextEntry to force fetching the next available entry
            nextEntry = null;
            
            return entry;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove() {
        iterator.remove();
        nextEntry = null;
    }

}

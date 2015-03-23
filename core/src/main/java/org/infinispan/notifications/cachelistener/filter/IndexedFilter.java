package org.infinispan.notifications.cachelistener.filter;

/**
 * A marker interface for filters that can be handled efficiently by a {@link FilterIndexingServiceProvider}. Such
 * filters can still be executed by calling the {@link #filterAndConvert} method but a {@link
 * FilterIndexingServiceProvider} could take advantage of this specific filter and execute it more efficiently by using
 * an alternative approach.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
public interface IndexedFilter<K, V, C> extends CacheEventFilterConverter<K, V, C> {
}

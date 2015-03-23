package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheEntryListenerInvocation;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

/**
 * A service provider for filter indexing services. This is supposed to perform the filtering operation in a more
 * efficient way than directly executing the filter by calling the {@link org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter#filterAndConvert(Object,
 * Object, Metadata, Object, Metadata, EventType)} method. Implementations are discovered via the {@link
 * java.util.ServiceLoader} or {@link org.infinispan.commons.util.ServiceFinder} mechanism. Implementations may have
 * their dependencies injected using the {@link org.infinispan.factories.annotations.Inject} annotation.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
public interface FilterIndexingServiceProvider {

   /**
    * Start the provider. This is called after the dependencies are injected.
    */
   void start();

   /**
    * Reports whether this provider supports the given filter type.
    *
    * @param indexedFilter an indexable filter
    * @return {@code true} if the filter is supported, {@code false} otherwise
    */
   boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter);

   /**
    * Starts handling an invocation that uses an {@link IndexedFilter}.
    *
    * @param invocation the invocation to handle
    * @param <K>        cache key type
    * @param <V>        cache value type
    * @return the wrapped invocation
    */
   <K, V> DelegatingCacheEntryListenerInvocation<K, V> interceptListenerInvocation(CacheEntryListenerInvocation<K, V> invocation);

   <K, V> void registerListenerInvocations(boolean isClustered, boolean isPrimaryOnly, boolean filterAndConvert,
                                           IndexedFilter<?, ?, ?> indexedFilter,
                                           Map<Class<? extends Annotation>, List<DelegatingCacheEntryListenerInvocation<K, V>>> listeners);

   /**
    * Stop the provider.
    */
   void stop();
}

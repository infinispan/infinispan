package org.infinispan.notifications.cachelistener.filter;

/**
 * Factory that can produce CacheEventFilters
 *
 * @author wburns
 * @since 7.0
 */
public interface CacheEventFilterFactory {

   /**
    * Retrieves a cache event filter instance from this factory.
    *
    * @param params parameters for the factory to be used to create filter instances
    * @return a filter instance for keys with their values
    */
   <K, V> CacheEventFilter<K, V> getFilter(Object[] params);

}

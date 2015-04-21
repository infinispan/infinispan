package org.infinispan.notifications.cachelistener.filter;

/**
 * Factory that can produce {@link CacheEventFilterConverter} instances.
 *
 * @since 7.2
 */
public interface CacheEventFilterConverterFactory {

   /**
    * Retrieves a cache event filter and converter instance from this factory.
    *
    * @param params parameters for the factory to be used to create converter instances
    * @return a {@link CacheEventFilterConverter} instance used
    * to filter and reduce size of event payloads
    */
   <K, V, C> CacheEventFilterConverter<K, V, C> getFilterConverter(Object[] params);

}

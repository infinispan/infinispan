package org.infinispan.notifications.cachelistener.filter;

/**
 * Factory that can produce CacheEventConverters
 *
 * @author wburns
 * @since 7.0
 */
public interface CacheEventConverterFactory {
   /**
    * Retrieves a cache event converter instance from this factory.
    *
    * @param params parameters for the factory to be used to create converter instances
    * @return a {@link org.infinispan.notifications.cachelistener.filter.CacheEventConverter} instance used
    * to reduce size of event payloads
    */
   <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params);
}

package org.infinispan.server.core;

import org.infinispan.AdvancedCache;

/**
 * Query facade SPI. This is not meant to be implemented by regular users. At most one implmentation can exist in
 * server's classpath.
 *
 * @author Galder Zamarreño
 * @author wburns
 * @since 9.0
 */
public interface QueryFacade {

   /**
    * Execute a query against a cache.
    *
    * @param cache the cache to execute the query
    * @param query the query, serialized using protobuf
    * @return the results, serialized using protobuf
    */
   byte[] query(AdvancedCache<?, ?> cache, byte[] query);
}

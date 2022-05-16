package org.infinispan.hotrod.impl.cache;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

/**
 * Contains information about cache topology including servers and owned segments.
 *
 * @since 14.0
 */
public interface CacheTopologyInfo {

   /**
    * @return The number of configured segments for the cache.
    */
   Integer getNumSegments();

   /**
    * @return Segments owned by each server.
    */
   Map<SocketAddress, Set<Integer>> getSegmentsPerServer();

   Integer getTopologyId();
}

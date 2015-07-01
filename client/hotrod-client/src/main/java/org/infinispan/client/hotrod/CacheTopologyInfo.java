package org.infinispan.client.hotrod;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

/**
 * Contains information about cache topology including servers and owned segments.
 *
 * @author gustavonalle
 * @since 8.0
 */
public interface CacheTopologyInfo {

   /**
    * @return The number of configured segments for the cache.
    */
   int getNumSegments();

   /**
    * @return Segments owned by each server.
    */
   Map<SocketAddress, Set<Integer>> getSegmentsPerServer();

}

package org.infinispan.server.hotrod

import org.infinispan.manager.CacheManager
import org.infinispan.server.core.transport.{Decoder, Encoder}
import org.jgroups.blocks.RequestOptions
import org.infinispan.server.core.{Logging, AbstractProtocolServer}
import org.infinispan.config.Configuration
import org.infinispan.config.Configuration.CacheMode
import org.infinispan.Cache

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class HotRodServer extends AbstractProtocolServer("HotRod") {

   import HotRodServer._

   override def getEncoder: Encoder = new HotRodEncoder

   override def getDecoder: Decoder = new HotRodDecoder(getCacheManager)

   override def start(host: String, port: Int, cacheManager: CacheManager, masterThreads: Int, workerThreads: Int, idleTimeout: Int) {
      super.start(host, port, cacheManager, masterThreads, workerThreads, idleTimeout)
      // If clustered, set up a cache for topology information
      if (cacheManager.getGlobalConfiguration.getTransportClass != null) {
         defineTopologyCacheConfig(cacheManager)
         val topologyCache: Cache[String, TopologyView] = cacheManager.getCache(TopologyCacheName)
         val currentView = topologyCache.get("view")
         if (currentView != null) {
            // TODO: If distribution configured, add hashcode of this address
            val newMembers = currentView.members ::: List(TopologyAddress(host, port, 0))
            val newView = TopologyView(currentView.topologyId + 1, newMembers)
            val replaced = topologyCache.replace("view", currentView, newView)
            if (!replaced) {
               // TODO: There was a concurrent view modification, get and try to install new view again.
            }
         } else {
            // TODO add check for distribution and if so, put the right hashcode
            val newMembers = List(TopologyAddress(host, port, 0))
            val newView = TopologyView(1, newMembers)
            val prev = topologyCache.putIfAbsent("view", newView)
            if (prev != null) {
               // TODO: There was a concurrent view modification, get and try to install new view again.
            }
         }
      }
   }

   protected def defineTopologyCacheConfig(cacheManager: CacheManager) {
      val topologyCacheConfig = new Configuration
      topologyCacheConfig.setCacheMode(CacheMode.REPL_SYNC)
      topologyCacheConfig.setSyncReplTimeout(10000) // Milliseconds
      topologyCacheConfig.setFetchInMemoryState(true) // State transfer required
      cacheManager.defineConfiguration(TopologyCacheName, topologyCacheConfig)
   }

}

object HotRodServer {
   val TopologyCacheName = "___hotRodTopologyCache"
}
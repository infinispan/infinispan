package org.infinispan.server.memcached

import org.infinispan.server.core.AbstractProtocolServer
import java.util.concurrent.Executors
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration
import org.infinispan.AdvancedCache
import org.infinispan.configuration.cache.ConfigurationBuilder

/**
 * Memcached server defining its decoder/encoder settings. In fact, Memcached does not use an encoder since there's
 * no really common headers between protocol operations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class MemcachedServer extends AbstractProtocolServer("Memcached") {
   type SuitableConfiguration = MemcachedServerConfiguration

   protected lazy val scheduler = Executors.newScheduledThreadPool(1)
   private var memcachedCache: AdvancedCache[String, Array[Byte]] = _

   override def startInternal(configuration: MemcachedServerConfiguration, cacheManager: EmbeddedCacheManager) {
      if (!cacheManager.cacheExists(configuration.cache)) {
         // Define the Memcached cache as clone of the default one
         cacheManager.defineConfiguration(configuration.cache,
            new ConfigurationBuilder().read(cacheManager.getDefaultCacheConfiguration).build())
      }
      memcachedCache = cacheManager.getCache[String, Array[Byte]](configuration.cache).getAdvancedCache
      super.startInternal(configuration, cacheManager)
   }

   override def getEncoder = null

   override def getDecoder: MemcachedDecoder =
      new MemcachedDecoder(memcachedCache, scheduler, transport)

   override def stop {
      super.stop
      scheduler.shutdown()
   }
}

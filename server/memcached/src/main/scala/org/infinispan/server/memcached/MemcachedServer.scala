package org.infinispan.server.memcached

import org.infinispan.server.core.AbstractProtocolServer
import org.infinispan.server.core.transport.{Decoder, Encoder}
import java.util.concurrent.{Executors, ScheduledExecutorService}
import org.infinispan.manager.{EmbeddedCacheManager, CacheContainer}
import java.util.Properties

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class MemcachedServer extends AbstractProtocolServer("Memcached") {

   protected lazy val scheduler = Executors.newScheduledThreadPool(1)

   override def start(p: Properties, cacheManager: EmbeddedCacheManager) {
      val properties = if (p == null) new Properties else p
      super.start(properties, cacheManager, 11211)
   }

   override def getEncoder: Encoder = null

   override def getDecoder: Decoder = new MemcachedDecoder(getCacheManager.getCache[String, MemcachedValue], scheduler)

   override def stop {
      super.stop
      scheduler.shutdown
   }
}
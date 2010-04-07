package org.infinispan.server.memcached

import org.infinispan.server.core.AbstractProtocolServer
import org.infinispan.server.core.transport.{Decoder, Encoder}
import org.infinispan.manager.CacheManager
import java.util.concurrent.{Executors, ScheduledExecutorService}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class MemcachedServer extends AbstractProtocolServer {

   protected lazy val scheduler = Executors.newScheduledThreadPool(1)

   override def getEncoder: Encoder = null

   override def getDecoder: Decoder = new MemcachedDecoder(getCacheManager, scheduler)

   override def stop {
      super.stop
      scheduler.shutdown
   }
}
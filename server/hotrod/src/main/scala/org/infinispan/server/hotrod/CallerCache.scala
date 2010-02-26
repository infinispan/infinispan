package org.infinispan.server.hotrod

import org.infinispan.manager.CacheManager
import org.infinispan.{Cache => InfinispanCache}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.0
 */

class CallerCache(val manager: CacheManager) extends Cache {

   override def put(c: StorageCommand): Reply.Value = {
      val cache: InfinispanCache[Array[Byte], Array[Byte]] = manager.getCache(c.cacheName)
      cache.put(c.key, c.value)
      Reply.Stored
   }

   override def get(c: RetrievalCommand): Reply.Value = {
      null
   }
}
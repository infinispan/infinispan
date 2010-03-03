package org.infinispan.server.hotrod

import org.infinispan.manager.CacheManager
import org.infinispan.{Cache => InfinispanCache}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.0
 */

class CallerCache(val manager: CacheManager) extends Cache {

   override def put(c: StorageCommand): Response = {
      val cache: InfinispanCache[Array[Byte], Array[Byte]] = manager.getCache(c.cacheName)
      cache.put(c.key, c.value)
      new Response(OpCodes.PutResponse, c.id, Status.Success)
   }

   override def get(c: RetrievalCommand): Response = {
      null
   }
}
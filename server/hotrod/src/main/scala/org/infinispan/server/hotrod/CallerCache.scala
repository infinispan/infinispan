package org.infinispan.server.hotrod

import org.infinispan.{Cache => InfinispanCache}
import org.infinispan.manager.{DefaultCacheManager, CacheManager}
import java.util.concurrent.TimeUnit

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.0
 */

class CallerCache(val manager: CacheManager) extends Cache {

   import CallerCache._

   override def put(c: StorageCommand): Response = {
      val cache = getCache(c.cacheName)
      val k = new Key(c.key)
      val v = new Value(c.value)
      (c.lifespan, c.maxIdle) match {
         case (0, 0) => cache.put(k, v)
         case (x, 0) => cache.put(k, v, toMillis(c.lifespan), TimeUnit.MILLISECONDS)
         case (x, y) => cache.put(k, v, toMillis(c.lifespan), TimeUnit.MILLISECONDS, c.maxIdle, TimeUnit.SECONDS)
      }
      new Response(OpCodes.PutResponse, c.id, Status.Success)
   }

   override def get(c: RetrievalCommand): Response = {
      val cache = getCache(c.cacheName)
      val value = cache.get(new Key(c.key))
      if (value != null)
         new RetrievalResponse(OpCodes.GetResponse, c.id, Status.Success, value.v)
      else
         new RetrievalResponse(OpCodes.GetResponse, c.id, Status.KeyDoesNotExist, null)
   }

   private def getCache(cacheName: String): InfinispanCache[Key, Value] = {
      if (cacheName == DefaultCacheManager.DEFAULT_CACHE_NAME)
         manager.getCache[Key, Value]
      else
         manager.getCache(cacheName)
   }

   /**
    * Transforms lifespan pass as seconds into milliseconds
    * following this rule:
    *
    * If lifespan is bigger than number of seconds in 30 days,
    * then it is considered unix time. After converting it to
    * milliseconds, we substract the current time in and the
    * result is returned.
    *
    * Otherwise it's just considered number of seconds from
    * now and it's returned in milliseconds unit.
    */
   private def toMillis(lifespan: Int) = {
      if (lifespan > SecondsInAMonth) TimeUnit.SECONDS.toMillis(lifespan) - System.currentTimeMillis
      else TimeUnit.SECONDS.toMillis(lifespan)
   }
}

object CallerCache {
   private val SecondsInAMonth = 60 * 60 * 24 * 30
}
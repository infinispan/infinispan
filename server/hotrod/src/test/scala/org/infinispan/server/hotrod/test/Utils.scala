package org.infinispan.server.hotrod.test

import java.util.concurrent.atomic.AtomicInteger
import org.infinispan.manager.CacheManager
import java.lang.reflect.Method
import org.infinispan.server.hotrod.{Logging, HotRodServer}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

trait Utils {

   import Utils._

   def host = "127.0.0.1"

   def createHotRodServer(manager: CacheManager): HotRodServer = {
      new HotRodServer(host, UniquePortThreadLocal.get.intValue, manager, 0, 0)
   }

   def k(m: Method, prefix: String): Array[Byte] = {
      val bytes: Array[Byte] = (prefix + m.getName).getBytes
      trace("String {0} is converted to {1} bytes", prefix + m.getName, bytes)
      bytes
   }

   def v(m: Method, prefix: String): Array[Byte] = k(m, prefix)

   def k(m: Method): Array[Byte] = k(m, "k-")

   def v(m: Method): Array[Byte] = v(m, "v-")

}

object Utils extends Logging

object UniquePortThreadLocal extends ThreadLocal[Int] {
   private val uniqueAddr = new AtomicInteger(21212)   
   override def initialValue: Int = uniqueAddr.getAndAdd(100)
}
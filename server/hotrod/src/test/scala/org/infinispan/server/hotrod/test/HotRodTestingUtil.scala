package org.infinispan.server.hotrod.test

import java.util.concurrent.atomic.AtomicInteger
import org.infinispan.manager.CacheManager
import java.lang.reflect.Method
import org.infinispan.server.hotrod.{HotRodServer}
import org.infinispan.server.core.Logging
import java.util.Arrays
import org.infinispan.server.hotrod.OperationStatus._
import org.testng.Assert._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

// TODO: convert to object so that mircea can use it
object HotRodTestingUtil extends Logging {

   import HotRodTestingUtil._

   def host = "127.0.0.1"

   def startHotRodServer(manager: CacheManager): HotRodServer =
      startHotRodServer(manager, UniquePortThreadLocal.get.intValue)

   def startHotRodServer(manager: CacheManager, port: Int): HotRodServer = {
      val server = new HotRodServer
      server.start(host, port, manager, 0, 0)
      server
   }

   def k(m: Method, prefix: String): Array[Byte] = {
      val bytes: Array[Byte] = (prefix + m.getName).getBytes
      trace("String {0} is converted to {1} bytes", prefix + m.getName, bytes)
      bytes
   }

   def v(m: Method, prefix: String): Array[Byte] = k(m, prefix)

   def k(m: Method): Array[Byte] = k(m, "k-")

   def v(m: Method): Array[Byte] = v(m, "v-")

   def assertStatus(status: OperationStatus, expected: OperationStatus): Boolean = {
      val isSuccess = status == expected
      assertTrue(isSuccess, "Status should have been '" + expected + "' but instead was: " + status)
      isSuccess
   }

   def assertSuccess(status: OperationStatus, expected: Array[Byte], actual: Array[Byte]): Boolean = {
      assertStatus(status, Success)
      val isSuccess = Arrays.equals(expected, actual)
      assertTrue(isSuccess)
      isSuccess
   }

   def assertKeyDoesNotExist(status: OperationStatus, actual: Array[Byte]): Boolean = {
      assertTrue(status == KeyDoesNotExist, "Status should have been 'KeyDoesNotExist' but instead was: " + status)
      assertNull(actual)
      status == KeyDoesNotExist
   }
   
} 

object UniquePortThreadLocal extends ThreadLocal[Int] {
   private val uniqueAddr = new AtomicInteger(11311)
   override def initialValue: Int = uniqueAddr.getAndAdd(100)
}
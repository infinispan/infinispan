package org.infinispan.server.hotrod.test

import java.util.concurrent.atomic.AtomicInteger
import org.infinispan.manager.CacheManager
import java.lang.reflect.Method
import org.infinispan.server.core.Logging
import java.util.Arrays
import org.infinispan.server.hotrod.OperationStatus._
import org.testng.Assert._
import org.infinispan.util.Util
import org.infinispan.server.hotrod.{ResponseWithPrevious, GetWithVersionResponse, GetResponse, HotRodServer}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
object HotRodTestingUtil extends Logging {

   import HotRodTestingUtil._

   def host = "127.0.0.1"

   def startHotRodServer(manager: CacheManager): HotRodServer =
      startHotRodServer(manager, UniquePortThreadLocal.get.intValue)

   def startHotRodServer(manager: CacheManager, port: Int): HotRodServer =
      startHotRodServer(manager, port, 0)

   def startHotRodServer(manager: CacheManager, port: Int, idleTimeout: Int): HotRodServer = {
      val server = new HotRodServer {
         override protected def defineTopologyCacheConfig(cacheManager: CacheManager) {
            // No-op since topology cache configuration comes defined by the test
         }
      }
      server.start(host, port, manager, 0, 0, idleTimeout)
      server
   }

   def startCrashingHotRodServer(manager: CacheManager, port: Int): HotRodServer = {
      val server = new HotRodServer {
         override protected def defineTopologyCacheConfig(cacheManager: CacheManager) {
            // No-op since topology cache configuration comes defined by the test
         }

         override protected def removeSelfFromTopologyView {
            // Empty to emulate a member that's crashed/unresponsive and has not executed removal,
            // but has been evicted from JGroups cluster.
         }
      }
      server.start(host, port, manager, 0, 0, 0)
      server
   }

   def k(m: Method, prefix: String): Array[Byte] = {
      val bytes: Array[Byte] = (prefix + m.getName).getBytes
      trace("String {0} is converted to {1} bytes", prefix + m.getName, Util.printArray(bytes, true))
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

   def assertSuccess(resp: GetResponse, expected: Array[Byte]): Boolean = {
      assertStatus(resp.status, Success)
      val isSuccess = Arrays.equals(expected, resp.data.get)
      assertTrue(isSuccess)
      isSuccess
   }

   def assertSuccess(resp: GetWithVersionResponse, expected: Array[Byte], expectedVersion: Int): Boolean = {
      assertTrue(resp.version != expectedVersion)
      assertSuccess(resp, expected)
   }

   def assertSuccess(resp: ResponseWithPrevious, expected: Array[Byte]): Boolean = {
      assertStatus(resp.status, Success)
      val isSuccess = Arrays.equals(expected, resp.previous.get)
      assertTrue(isSuccess)
      isSuccess
   }

   def assertKeyDoesNotExist(resp: GetResponse): Boolean = {
      val status = resp.status
      assertTrue(status == KeyDoesNotExist, "Status should have been 'KeyDoesNotExist' but instead was: " + status)
      assertEquals(resp.data, None)
      status == KeyDoesNotExist
   }

} 

object UniquePortThreadLocal extends ThreadLocal[Int] {
   private val uniqueAddr = new AtomicInteger(11311)
   override def initialValue: Int = uniqueAddr.getAndAdd(100)
}
package org.infinispan.server.hotrod

import org.infinispan.test.MultipleCacheManagersTest
import org.infinispan.config.Configuration
import java.lang.reflect.Method
import org.testng.annotations.{AfterClass, Test}
import test.HotRodClient
import test.HotRodTestingUtil._
import org.infinispan.server.hotrod.OperationStatus._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

@Test(groups = Array("functional"), testName = "server.hotrod.ClusterTest")
class HotRodReplicationTest extends MultipleCacheManagersTest {
   private val cacheName = "hotRodReplSync"
   private[this] var servers: List[HotRodServer] = List()
   private[this] var clients: List[HotRodClient] = List()

   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers {
      var replSync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC)
      createClusteredCaches(2, cacheName, replSync)
      servers = startHotRodServer(cacheManagers.get(0)) :: servers
      servers = startHotRodServer(cacheManagers.get(1), servers.head.getPort + 50) :: servers
      servers.foreach {s =>
         clients = new HotRodClient("127.0.0.1", s.getPort, cacheName) :: clients
      }
   }

   @AfterClass(alwaysRun = true)
   override def destroy {
      super.destroy
      log.debug("Test finished, close Hot Rod server", null)
      clients.foreach(_.stop)
      servers.foreach(_.stop)
   }

   def tesReplicatedPut(m: Method) {
      val putSt = clients.head.put(k(m) , 0, 0, v(m))
      assertStatus(putSt, Success)
      val (getSt, actual) = clients.tail.head.get(k(m), 0)
      assertSuccess(getSt, v(m), actual)
   }

   def tesReplicatedPutIfAbsent(m: Method) {
      val (getSt, actual) = clients.head.assertGet(m)
      assertKeyDoesNotExist(getSt, actual)
      val (getSt2, actual2) = clients.tail.head.assertGet(m)
      assertKeyDoesNotExist(getSt2, actual2)
      val putSt = clients.head.putIfAbsent(k(m) , 0, 0, v(m))
      assertStatus(putSt, Success)
      val (getSt3, actual3) = clients.tail.head.get(k(m), 0)
      assertSuccess(getSt3, v(m), actual3)
      val putSt2 = clients.tail.head.putIfAbsent(k(m) , 0, 0, v(m, "v2-"))
      assertStatus(putSt2, OperationNotExecuted)
   }

   def testReplicatedReplace(m: Method) {
      val status = clients.head.replace(k(m), 0, 0, v(m))
      assertStatus(status, OperationNotExecuted)
      val status2 = clients.tail.head.replace(k(m), 0, 0, v(m))
      assertStatus(status2, OperationNotExecuted)
      clients.tail.head.assertPut(m)
      val status3 = clients.tail.head.replace(k(m), 0, 0, v(m, "v1-"))
      assertStatus(status3, Success)
      val (getSt, actual) = clients.head.assertGet(m)
      assertSuccess(getSt, v(m, "v1-"), actual)
      val status4 = clients.head.replace(k(m), 0, 0, v(m, "v2-"))
      assertStatus(status4, Success)
      val (getSt2, actual2) = clients.tail.head.assertGet(m)
      assertSuccess(getSt2, v(m, "v2-"), actual2)
   }

}
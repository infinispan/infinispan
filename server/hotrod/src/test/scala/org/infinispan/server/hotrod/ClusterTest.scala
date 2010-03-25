//package org.infinispan.server.hotrod
//
//import org.infinispan.test.MultipleCacheManagersTest
//import org.infinispan.config.Configuration
//import org.jboss.netty.channel.Channel
//import test.{Utils, Client}
//import java.lang.reflect.Method
//import org.infinispan.manager.CacheManager
//import org.testng.annotations.{BeforeClass, AfterClass, Test}
//import org.infinispan.config.Configuration.CacheMode
//import org.infinispan.context.Flag
//import org.infinispan.server.hotrod.Status._
//
///**
// * // TODO: Document this
// * @author Galder Zamarreño
// * @since
// */
//
//@Test(groups = Array("functional"), testName = "server.hotrod.ClusterTest")
//class ClusterTest extends MultipleCacheManagersTest with Utils with Client {
//   private val cacheName = "hotRodReplSync"
//   private[this] var servers: List[HotRodServer] = List()
//   private[this] var channels: List[Channel] = List()
//
//   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
//   override def createCacheManagers {
//      var replSync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC)
//      createClusteredCaches(2, cacheName, replSync)
//      servers = startHotRodServer(cacheManagers.get(0)) :: servers
//      servers = startHotRodServer(cacheManagers.get(1), servers.head.port + 50) :: servers
//      servers.foreach {
//         s => s.start
//         channels = connect("127.0.0.1", s.port) :: channels
//      }
//   }
//
//   @AfterClass(alwaysRun = true)
//   override def destroy {
//      super.destroy
//      log.debug("Test finished, close Hot Rod server", null)
//      servers.foreach(_.stop)
//   }
//
//   def tesReplicatedPut(m: Method) {
//      val putSt = put(channels.head, cacheName, k(m) , 0, 0, v(m))
//      assertStatus(putSt, Success)
//      val (getSt, actual) = get(channels.tail.head, cacheName, k(m), null)
//      assertSuccess(getSt, v(m), actual)
//   }
//
//   def tesLocalOnlyPut(m: Method) {
//      val putSt = put(channels.head, cacheName, k(m) , 0, 0, v(m), Set(Flag.CACHE_MODE_LOCAL))
//       assertStatus(putSt, Success)
//      val (getSt, actual) = get(channels.tail.head, cacheName, k(m), null)
//      assertKeyDoesNotExist(getSt, actual)
//   }
//
//   // todo: test multiple flags
//}
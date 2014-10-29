package org.infinispan.server.hotrod

import org.infinispan.configuration.cache.CacheMode
import org.infinispan.test.MultipleCacheManagersTest
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.testng.annotations.Test

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// Do not remove, otherwise getDefaultClusteredConfig is not found
import org.infinispan.server.core.test.ServerTestingUtil._
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.server.hotrod.test.UniquePortThreadLocal
import org.infinispan.test.AbstractCacheTest._

/**
 * Tests concurrent Hot Rod server startups
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodConcurrentStartTest")
class HotRodConcurrentStartTest extends MultipleCacheManagersTest {
   private val numberOfServers = 2
   private val cacheName = "hotRodConcurrentStart"

   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers() {
      for (i <- 0 until numberOfServers) {
         val cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration())
         cacheManagers.add(cm)
         val cfg = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false))
         cm.defineConfiguration(cacheName, cfg.build())
      }
   }

   def testConcurrentStartup() {
      val initialPort = UniquePortThreadLocal.get.intValue

      implicit val forExecutionContext = new ExecutionContext {
         override def execute(runnable: Runnable): Unit = fork(runnable)
         override def reportFailure(cause: Throwable): Unit = throw cause
      }

      val futures: Seq[Future[HotRodServer]] = (1 to numberOfServers).map {
         case 1 =>  Future(startHotRodServerWithDelay(getCacheManagers.get(0), initialPort, 10000))
         case i =>  Future(startHotRodServer(getCacheManagers.get(i - 1), initialPort + (i * 10)))
      }

      val results = futures.map(f => Try(Await.result(f, 1 minute)))
      val success = results.collect { case Success(s) => s}
      val errors = results.collect { case Failure(e) => e}
      if (success.isEmpty) {
         errors.foreach(log.error("Server failed to start", _))
         throw new AssertionError("All serves failed to start, see error log messages for more info")
      }
      // kill servers that started
      success map killServer
      // throw error if any
      errors map(throw _)
   }

}
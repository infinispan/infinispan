package org.infinispan.server.memcached

import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.server.core.ConnectionStatsTest._
import org.testng.annotations.Test
import javax.management.{MBeanServer, MBeanServerFactory}
import org.infinispan.jmx.MBeanServerLookup
import java.util.Properties

/**
 * Tests whether statistics of clustered Memcached instances
 * are calculated correctly.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = Array("unstable"), testName = "server.memcached.MemcachedClusteredStatsTest",
   description = "original group: functional - randomly fails with: expected [2] but found [1])")
class MemcachedClusteredStatsTest extends MemcachedMultiNodeTest {

   private val jmxDomain = classOf[MemcachedClusteredStatsTest].getSimpleName

   private val mbeanServerLookup = new ProvidedMBeanServerLookup(
      MBeanServerFactory.createMBeanServer)

   protected def createCacheManager(index: Int): EmbeddedCacheManager = {
      val builder = new ConfigurationBuilder
      builder.clustering().cacheMode(CacheMode.REPL_SYNC)
      // Per-thread mbean server won't work here because the registration will
      // happen in the 'main' thread and the remote call will try to resolve it
      // in a lookup instance associated with an 'OOB-' thread.
      TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(null,
         jmxDomain + "-" + index, true, false, builder, mbeanServerLookup)
   }

   def testSingleConnectionPerServer() {
      testGlobalConnections(jmxDomain + "-0", "Memcached", 2,
         mbeanServerLookup.getMBeanServer(null))
   }

   class ProvidedMBeanServerLookup(mbeanServer: MBeanServer) extends MBeanServerLookup {
      def getMBeanServer(properties: Properties) = mbeanServer
   }

}

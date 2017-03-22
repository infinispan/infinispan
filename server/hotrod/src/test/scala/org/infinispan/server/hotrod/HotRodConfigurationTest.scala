package org.infinispan.server.hotrod

import java.util.function.Consumer

import org.infinispan.configuration.cache.{ClusterLoaderConfiguration, Configuration}
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.test.Stoppable
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.test.AbstractInfinispanTest
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.testng.Assert._
import org.testng.annotations.Test

/**
 * Test to verify that configuration changes are reflected in backend caches.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodConfigurationTest")
class HotRodConfigurationTest extends AbstractInfinispanTest {

   def testUserDefinedTimeouts() {
      val builder = new HotRodServerConfigurationBuilder
      builder.topologyLockTimeout(26000).topologyReplTimeout(31000)
      withClusteredServer(builder) { (cfg, distSyncTimeout) =>
         assertEquals(cfg.locking().lockAcquisitionTimeout(), 26000)
         assertEquals(cfg.clustering().remoteTimeout(), 31000)
         assertTrue(cfg.clustering().stateTransfer().fetchInMemoryState())
         assertEquals(cfg.clustering().stateTransfer().timeout(), 31000 + distSyncTimeout)
         assertTrue(cfg.persistence().stores().isEmpty)
      }
   }

   def testLazyLoadTopology() {
      val builder = new HotRodServerConfigurationBuilder
      builder.topologyStateTransfer(false).topologyReplTimeout(43000)
      withClusteredServer(builder) { (cfg, distSyncTimeout) =>
         assertEquals(cfg.clustering().remoteTimeout(), 43000)
         assertTrue(cfg.clustering().stateTransfer().fetchInMemoryState())
         val clcfg = cfg.persistence().stores().get(0).asInstanceOf[ClusterLoaderConfiguration]
         assertNotNull(clcfg)
         assertEquals(clcfg.remoteCallTimeout(), 43000)
      }
   }

   private def withClusteredServer(builder: HotRodServerConfigurationBuilder) (assert: (Configuration, Long) => Unit) {

      Stoppable.useCacheManager(TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration()), new Consumer[EmbeddedCacheManager] {
         override def accept(cm: EmbeddedCacheManager): Unit = {
            Stoppable.useServer(startHotRodServer(cm, serverPort, builder), new Consumer[HotRodServer] {
               override def accept(server: HotRodServer): Unit = {
                  val cfg = cm.getCacheConfiguration(server.getConfiguration.topologyCacheName)
                  assert(cfg, cm.getCacheManagerConfiguration.transport().distributedSyncTimeout())
               }
            })
         }
      })
   }

}

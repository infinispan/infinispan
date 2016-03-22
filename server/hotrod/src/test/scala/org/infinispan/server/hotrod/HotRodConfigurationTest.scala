package org.infinispan.server.hotrod

import test.UniquePortThreadLocal
import test.HotRodTestingUtil._
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.testng.Assert._
import org.testng.annotations.Test
import org.infinispan.server.core.test.Stoppable
import org.infinispan.configuration.cache.{ConfigurationBuilder, Configuration}
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.configuration.cache.ClusterLoaderConfiguration
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration
import org.infinispan.util.concurrent.IsolationLevel
import org.infinispan.commons.CacheConfigurationException
import org.infinispan.configuration.cache.VersioningScheme

/**
 * Test to verify that configuration changes are reflected in backend caches.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodConfigurationTest")
class HotRodConfigurationTest {

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

   @Test(expectedExceptions = Array(classOf[CacheConfigurationException]))
   def testRepeatableReadIsolationLevelValidation() {
      validateIsolationLevel(IsolationLevel.REPEATABLE_READ, false)
   }

   @Test(expectedExceptions = Array(classOf[CacheConfigurationException]))
   def testSerializableIsolationLevelValidation() {
      validateIsolationLevel(IsolationLevel.SERIALIZABLE, false)
   }
   
   def testRepeatableReadIsolationLevelWithSkewCheckValidation() {
      validateIsolationLevel(IsolationLevel.REPEATABLE_READ, true)
   }

   private def withClusteredServer(builder: HotRodServerConfigurationBuilder) (assert: (Configuration, Long) => Unit) {
      Stoppable.useCacheManager(TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration())) { cm =>
         Stoppable.useServer(startHotRodServer(cm, UniquePortThreadLocal.get.intValue, builder)) { server =>
            val cfg = cm.getCache(HotRodServerConfiguration.TOPOLOGY_CACHE_NAME_PREFIX).getCacheConfiguration
            assert(cfg, cm.getCacheManagerConfiguration.transport().distributedSyncTimeout())
         }
      }
   }

   private def validateIsolationLevel(isolationLevel: IsolationLevel, writeSkew: Boolean) {
      val hotRodBuilder = new HotRodServerConfigurationBuilder
      val builder = new ConfigurationBuilder()
      builder.locking().isolationLevel(isolationLevel).writeSkewCheck(writeSkew)
      if (writeSkew) {
        builder.versioning().enable().scheme(VersioningScheme.SIMPLE)
      }
      Stoppable.useCacheManager(TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration(builder))) {
         cm =>
            Stoppable.useServer(startHotRodServer(cm, UniquePortThreadLocal.get.intValue, hotRodBuilder)) { server => {} }
      }
   }

}
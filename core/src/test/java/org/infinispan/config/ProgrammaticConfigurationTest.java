package org.infinispan.config;

import java.util.List;

import org.infinispan.config.Configuration.CacheMode;
import org.infinispan.config.Configuration.ClusteringConfig;
import org.infinispan.config.Configuration.EvictionConfig;
import org.infinispan.config.Configuration.ExpirationConfig;
import org.infinispan.config.Configuration.LoadersConfig;
import org.infinispan.config.Configuration.LockingConfig;
import org.infinispan.config.GlobalConfiguration.ExecutorFactoryConfig;
import org.infinispan.config.GlobalConfiguration.GlobalJmxStatisticsConfig;
import org.infinispan.config.GlobalConfiguration.TransportConfig;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.loaders.file.FileCacheStoreConfig;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "config.ProgrammaticConfigurationTest")
public class ProgrammaticConfigurationTest extends AbstractInfinispanTest {

   public void testDefiningConfigurationOverridingConsistentHashClass() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      Configuration c = new Configuration();
      c.setConsistentHashClass("org.infinispan.distribution.DefaultConsistentHash");
      Configuration oneCacheConfiguration = cm.defineConfiguration("oneCache", c);
      assert oneCacheConfiguration.equals(c);
   }

   public void testGlobalConfiguration() {
      GlobalConfiguration gc = new GlobalConfiguration();
      GlobalJmxStatisticsConfig jmxStatistics = gc.configureGlobalJmxStatistics();
      jmxStatistics.allowDuplicateDomains(true).enabled(true);

      TransportConfig transport = gc.configureTransport();
      transport.clusterName("blah").machineId("id").rackId("rack").strictPeerToPeer(true);

      ExecutorFactoryConfig<ExecutorFactory> asyncTransportExecutor = gc.configureAsyncTransportExecutor();
      asyncTransportExecutor.factory(DefaultExecutorFactory.class).addProperty("blah", "blah");

      assert gc.isAllowDuplicateDomains();
      assert gc.isExposeGlobalJmxStatistics();

      assert gc.getMachineId().equals("id");
      assert gc.getRackId().equals("rack");
      assert gc.getClusterName().equals("blah");

      assert gc.getAsyncTransportExecutorFactoryClass().equals(
               DefaultExecutorFactory.class.getName());
   }
   
   public void testConfiguration() {
      Configuration c = new Configuration();
      LockingConfig locking = c.configureLocking();
      locking.isolationLevel(IsolationLevel.REPEATABLE_READ).lockAcquisitionTimeout(1000L);
      
      ExpirationConfig expiration = c.configureExpiration();
      expiration.lifespan(1000L).maxIdle(1000L);
      
      EvictionConfig eviction = c.configureEviction();
      eviction.maxEntries(1000).strategy(EvictionStrategy.LRU);
      
      ClusteringConfig clustering = c.configureClustering();
      clustering.configureAsync().asyncMarshalling(true).replQueueInterval(1000L);
      clustering.configureHash().rehashEnabled(true).rehashRpcTimeout(1000L);
      clustering.configureL1().enabled(true).onRehash(true).lifespan(1000L);
      clustering.configureStateRetrieval().alwaysProvideInMemoryState(true).initialRetryWaitTime(1000L);
      clustering.mode(CacheMode.DIST_SYNC);
      
      LoadersConfig loaders = c.configureLoaders();
      loaders.passivation(true).preload(true).shared(false);
      FileCacheStoreConfig fcsc = new FileCacheStoreConfig();
      fcsc.configureAsyncStore().enabled(true).flushLockTimeout(1000L);
      fcsc.configureSingletonStore().pushStateTimeout(1000L).enabled(true);
      loaders.addCacheLoaderConfig(fcsc);
      
      c.configureTransaction().cacheStopTimeout(1000).syncCommitPhase(true);
      c.configureInterceptors().addCustomInterceptor(new CustomInterceptorConfig());
      
      assert c.getIsolationLevel().equals(IsolationLevel.REPEATABLE_READ);
      assert c.getLockAcquisitionTimeout() == 1000;
      
      assert c.getEvictionStrategy().equals(EvictionStrategy.LRU);
      assert c.getEvictionMaxEntries() == 1000;
      
      assert c.isUseAsyncMarshalling();
      assert c.getReplQueueInterval() == 1000;
      assert c.isRehashEnabled();
      assert c.getRehashRpcTimeout() == 1000;
      
      assert c.isL1CacheEnabled() && c.isL1OnRehash() && c.getL1Lifespan()==1000;
      assert c.isAlwaysProvideInMemoryState() && c.getStateRetrievalInitialRetryWaitTime()==1000;
      
      CacheLoaderManagerConfig managerConfig = c.getCacheLoaderManagerConfig();
      assert managerConfig.isPassivation() && managerConfig.isPreload() && !managerConfig.isShared();
      assert managerConfig.getCacheLoaderConfigs().size() == 1;
      
      List<CacheLoaderConfig> loaderConfigs = managerConfig.getCacheLoaderConfigs();
      FileCacheStoreConfig cacheLoaderConfig = (FileCacheStoreConfig) loaderConfigs.get(0);
      AsyncStoreConfig storeConfig = cacheLoaderConfig.getAsyncStoreConfig();
      assert storeConfig.isEnabled() && storeConfig.getFlushLockTimeout() ==1000;
      SingletonStoreConfig singletonStoreConfig = cacheLoaderConfig.getSingletonStoreConfig();
      assert singletonStoreConfig.getPushStateTimeout() == 1000 && singletonStoreConfig.isSingletonStoreEnabled();
            
      assert c.getCacheStopTimeout() == 1000 && c.isSyncCommitPhase();       
   }
}

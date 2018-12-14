package org.infinispan.lock;

import static org.infinispan.lock.impl.ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.configuration.ClusteredLockManagerConfigurationBuilder;
import org.infinispan.lock.configuration.Reliability;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;

public abstract class BaseClusteredLockSplitBrainTest extends BasePartitionHandlingTest {

   protected Reliability reliability = Reliability.CONSISTENT;
   protected int numOwner = -1;

   public BaseClusteredLockSplitBrainTest() {
      this.numMembersInCluster = 6;
      this.cacheMode = null;
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder dcc = cacheConfiguration();
      dcc.clustering().cacheMode(CacheMode.REPL_SYNC).partitionHandling().whenSplit(partitionHandling);
      createClusteredCaches(numMembersInCluster, dcc, new TransportFlags().withFD(true).withMerge(true));
      waitForClusterToForm(CLUSTERED_LOCK_CACHE_NAME);
   }

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder builder, TransportFlags flags) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();

      gcb.addModule(ClusteredLockManagerConfigurationBuilder.class)
            .numOwner(numOwner)
            .reliability(reliability);

      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(false, gcb, builder, flags, false);
      amendCacheManagerBeforeStart(cm);
      cacheManagers.add(cm);
      cm.start();
      return cm;
   }

   protected boolean availabilityExceptionRaised(ClusteredLockManager clm) {
      Exception ex = null;
      try {
         clm.defineLock(getLockName());
      } catch (AvailabilityException a) {
         ex = a;
      }
      return ex != null;
   }

   protected abstract String getLockName();
}

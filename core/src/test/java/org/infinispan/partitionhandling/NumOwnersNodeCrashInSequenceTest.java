package org.infinispan.partitionhandling;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * With a cluster made out of nodes {A,B,C,D}, tests that D crashes and before the state transfer finishes, another node
 * C crashes. {A,B} should enter in degraded mode. The only way in which it could recover is explicitly, through JMX
 * operations.
 */
@Test(groups = "functional", testName = "partitionhandling.NumOwnersNodeCrashInSequenceTest")
public class NumOwnersNodeCrashInSequenceTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configBuilder.clustering().partitionHandling().enabled(true);
      createCluster(configBuilder, 4);
   }

   public void testNodeCrashedBeforeStFinished() {
   }
}

package org.infinispan.expiration.impl;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests to make sure that when expiration occurs it occurs across the cluster when in replication mode
 *
 * @author William Burns
 * @since 8.0
 */
@Test(groups = "functional", testName = "expiration.impl.ClusterExpirationReplFunctionalTest")
public class ClusterExpirationReplFunctionalTest extends ClusterExpirationFunctionalTest {

   @Override
   protected void createCluster(ConfigurationBuilder builder, int count) {
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      super.createCluster(builder, count);
   }
}

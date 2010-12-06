package org.infinispan.tx.dld;

import org.infinispan.config.Configuration;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (groups = "functional", testName = "tx.dld.SameKeyDeadlockDistributionTest")
public class SameKeyDeadlockDistributionTest extends SameKeyDeadlockReplicationTest {
   @Override
   protected Configuration getConfiguration() {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      //we have two owners - this guarantees that lock acquisition will cause an deadlock
      config.setNumOwners(2);
      return config;
   }
}

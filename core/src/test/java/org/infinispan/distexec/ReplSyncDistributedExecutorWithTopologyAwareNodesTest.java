package org.infinispan.distexec;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Tests for verifying Distributed Executors for REPL_SYNC cache mode, with Topology Aware nodes.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.ReplSyncDistributedExecutorWithTopologyAwareNodesTest")
public class ReplSyncDistributedExecutorWithTopologyAwareNodesTest extends DistributedExecutorWithTopologyAwareNodesTest {

   public CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }
}
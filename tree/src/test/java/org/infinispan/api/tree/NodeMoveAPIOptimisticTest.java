package org.infinispan.api.tree;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Exercises and tests the new move() api using optimistic locking and write skew check.
 *
 * @author anistor@redhat.com
 */
@Test(groups = "functional", testName = "api.tree.NodeMoveAPIOptimisticTest")
public class NodeMoveAPIOptimisticTest extends BaseNodeMoveAPITest {

   @Override
   protected ConfigurationBuilder createConfigurationBuilder() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.invocationBatching().enable()
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis())
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .transaction().lockingMode(LockingMode.OPTIMISTIC)
            .locking().writeSkewCheck(true)
            .versioning().enable().scheme(VersioningScheme.SIMPLE);
      return cb;
   }
}

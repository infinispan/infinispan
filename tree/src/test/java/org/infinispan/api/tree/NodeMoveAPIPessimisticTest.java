package org.infinispan.api.tree;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Exercises and tests the new move() api using pessimistic locking.
 *
 * @author anistor@redhat.com
 */
@Test(groups = "functional", testName = "api.tree.NodeMoveAPIPessimisticTest")
public class NodeMoveAPIPessimisticTest extends BaseNodeMoveAPITest {

   @Override
   protected ConfigurationBuilder createConfigurationBuilder() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.invocationBatching().enable()
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis())
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .transaction().lockingMode(LockingMode.PESSIMISTIC);
      return cb;
   }
}

package org.infinispan.tx.recovery;

import org.infinispan.config.Configuration;
import org.infinispan.tx.dld.DldEagerLockingDistributedTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.recovery.DldEagerLockingAndRecoveryDistributedTest")
public class DldEagerLockingAndRecoveryDistributedTest extends DldEagerLockingDistributedTest {
   @Override
   protected Configuration createConfiguration() {
      return super.createConfiguration().fluent().transaction().recovery().build();
   }
}

package org.infinispan.tx.recovery;

import org.infinispan.config.Configuration;
import org.infinispan.tx.dld.DldPessimisticLockingDistributedTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.recovery.DldPessimisticLockingAndRecoveryDistributedTest")
public class DldPessimisticLockingAndRecoveryDistributedTest extends DldPessimisticLockingDistributedTest {
   @Override
   protected Configuration createConfiguration() {
      return super.createConfiguration().fluent().transaction().recovery().build();
   }
}

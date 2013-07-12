package org.infinispan.tx.recovery;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.tx.dld.DldPessimisticLockingDistributedTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.recovery.DldPessimisticLockingAndRecoveryDistributedTest")
public class DldPessimisticLockingAndRecoveryDistributedTest extends DldPessimisticLockingDistributedTest {
   @Override
   protected ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder configuration = super.createConfiguration();
      configuration.transaction().recovery().enable();
      return configuration;
   }
}

package org.infinispan.tx.recovery;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.tx.dld.DldPessimisticLockingReplicationTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test (groups = "functional", testName = "tx.recovery.DldEagerLockingAndRecoveryReplicationTest")
public class DldEagerLockingAndRecoveryReplicationTest extends DldPessimisticLockingReplicationTest {

   protected ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder configurationBuilder = super.createConfiguration();
      configurationBuilder.transaction().recovery().enable();
      return configurationBuilder;
   }
}

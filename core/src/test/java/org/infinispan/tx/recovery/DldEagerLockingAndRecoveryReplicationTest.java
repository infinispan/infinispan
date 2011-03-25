package org.infinispan.tx.recovery;

import org.infinispan.config.Configuration;
import org.infinispan.tx.dld.DldEagerLockingReplicationTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test (groups = "functional", testName = "tx.recovery.DldEagerLockingAndRecoveryReplicationTest")
public class DldEagerLockingAndRecoveryReplicationTest extends DldEagerLockingReplicationTest {

   protected Configuration getConfiguration() throws Exception {
      return super.getConfiguration().fluent().transaction().recovery().build();
   }
}

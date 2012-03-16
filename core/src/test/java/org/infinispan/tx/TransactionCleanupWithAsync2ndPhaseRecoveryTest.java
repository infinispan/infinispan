package org.infinispan.tx;

import org.infinispan.config.Configuration;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "TransactionReleaseWithAsync2ndPhaseRecoveryTest")
public class TransactionCleanupWithAsync2ndPhaseRecoveryTest extends TransactionCleanupWithAsync2ndPhaseTest {

   protected Configuration getConfiguration() {
      final Configuration dcc = super.getConfiguration();
      dcc.fluent().transaction().recovery();
      return dcc;
   }

}

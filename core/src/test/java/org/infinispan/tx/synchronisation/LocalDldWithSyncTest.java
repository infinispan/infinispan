package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.tx.dld.LocalDeadlockDetectionTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.synchronization.LocalDldWithSyncTest")
public class LocalDldWithSyncTest extends LocalDeadlockDetectionTest {

   @Override
   protected Configuration createConfig() {
      Configuration config = super.createConfig();
      config.configureTransaction().useSynchronization(true);
      return config;
   }
}

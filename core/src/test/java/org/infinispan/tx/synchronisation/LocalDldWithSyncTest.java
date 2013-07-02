package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.tx.dld.LocalDeadlockDetectionTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.synchronisation.LocalDldWithSyncTest")
public class LocalDldWithSyncTest extends LocalDeadlockDetectionTest {

   @Override
   protected Configuration createConfig() {
      Configuration config = super.createConfig();
      config.fluent().transaction().useSynchronization(true);
      return config;
   }
}

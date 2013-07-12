package org.infinispan.tx.synchronisation;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.tx.dld.LocalDeadlockDetectionTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.synchronisation.LocalDldWithSyncTest")
public class LocalDldWithSyncTest extends LocalDeadlockDetectionTest {

   @Override
   protected ConfigurationBuilder createConfig() {
      ConfigurationBuilder config = super.createConfig();
      config.transaction().useSynchronization(true);
      return config;
   }
}

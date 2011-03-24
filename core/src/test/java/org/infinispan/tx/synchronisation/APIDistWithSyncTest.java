package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.lock.APIDistTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.synchronization.APIDistWithSyncTest")
public class APIDistWithSyncTest extends APIDistTest {
   @Override
   protected Configuration createConfig() {
      Configuration config = super.createConfig();
      config.configureTransaction().useSynchronization(true);
      return config;
   }
}

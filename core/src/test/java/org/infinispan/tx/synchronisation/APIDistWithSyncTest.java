package org.infinispan.tx.synchronisation;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lock.APIDistTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.synchronisation.APIDistWithSyncTest")
public class APIDistWithSyncTest extends APIDistTest {
   @Override
   protected ConfigurationBuilder createConfig() {
      ConfigurationBuilder config = super.createConfig();
      config.transaction().useSynchronization(true);
      return config;
   }
}

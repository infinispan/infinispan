package org.infinispan.remoting;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Verifies remote exception handling when state transfer is enabled.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "remoting.RemoteExceptionHandlingWithStateTransferTest")
public class RemoteExceptionHandlingWithStateTransferTest extends TransportSenderExceptionHandlingTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      config.clustering().stateTransfer().fetchInMemoryState(true);
      createClusteredCaches(2, "replSync", FailureTypeSCI.INSTANCE, config);
   }
}

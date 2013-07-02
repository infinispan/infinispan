package org.infinispan.tx.recovery.admin;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.admin.ForgetReplicationTest")
public class ForgetReplicationTest extends ForgetTest {

   @Override
   protected ConfigurationBuilder defaultRecoveryConfig() {
      ConfigurationBuilder configuration = super.defaultRecoveryConfig();
      configuration.clustering().cacheMode(CacheMode.REPL_SYNC);
      return configuration;
   }
}

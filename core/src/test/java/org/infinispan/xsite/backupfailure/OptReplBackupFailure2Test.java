package org.infinispan.xsite.backupfailure;

import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.backupfailure.OptReplBackupFailure2Test")
public class OptReplBackupFailure2Test extends NonTxBackupFailureTest {

   public OptReplBackupFailure2Test() {
      lonBackupFailurePolicy = BackupFailurePolicy.FAIL;
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }
}

package org.infinispan.tx.recovery;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.recovery.RecoveryWithDefaultCacheReplTest")
@CleanupAfterMethod
public class RecoveryWithDefaultCacheReplTest extends RecoveryWithDefaultCacheDistTest {

   @Override
   protected ConfigurationBuilder configure() {
      ConfigurationBuilder config = super.configure();
      config.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .clustering().cacheMode(CacheMode.REPL_SYNC);
      return config;
   }
}

package org.infinispan.tx.recovery;

import org.infinispan.config.Configuration;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.recovery.RecoveryWithDefaultCacheReplTest")
public class RecoveryWithDefaultCacheReplTest extends RecoveryWithDefaultCacheDistTest {
   @Override
   protected Configuration configure() {
      Configuration config = super.configure();
      config.configureTransaction().transactionManagerLookupClass(DummyTransactionManagerLookup.class);
      config.configureClustering().mode(Configuration.CacheMode.REPL_SYNC);
      return config;
   }
}

package org.infinispan.distribution.rehash;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Tests that state transfer can't overwrite a value written by a command during state transfer.
 * See https://issues.jboss.org/browse/ISPN-3443
 *
 * @author Dan Berindei
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.PessimisticStateTransferOverwritingValueTest")
public class OptimisticStateTransferOverwritingValueTest extends NonTxStateTransferOverwritingValueTest {
   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.OPTIMISTIC);
      return c;
   }
}

package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

/**
 * @author William Burns
 * @since 6.0
 */
@Test (groups = "functional", testName = "tx.ReadOnlyRepeatableReadTxTest")
@CleanupAfterMethod
public class ReadOnlyRepeatableReadTxTest extends ReadOnlyTxTest {
   protected void configure(ConfigurationBuilder builder) {
      super.configure(builder);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
   }
}

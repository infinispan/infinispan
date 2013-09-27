package org.infinispan.tx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.jgroups.util.Util.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

/**
 * This test is to ensure that values in the context are properly counted for various cache operations
 *
 * @author wburns
 * @since 6.0
 */
@Test (groups = "functional", testName = "tx.ContextAffectsTransactionRepeatableReadTest")
public class ContextAffectsTransactionRepeatableReadTest extends ContextAffectsTransactionReadCommittedTest {
   @Override
   protected void configure(ConfigurationBuilder builder) {
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
   }
}

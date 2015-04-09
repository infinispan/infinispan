package org.infinispan.commands;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Stress test designed to test to verify that put map works properly under constant
 * topology changes in a transactional cache
 *
 * @author wburns
 * @since 7.2
 */
@Test(groups = "stress", testName = "commands.PutMapCommandTxStressTest")
public class PutMapCommandTxStressTest extends PutMapCommandStressTest {
   @Override
   protected void configure(ConfigurationBuilder builder) {
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
   }
}

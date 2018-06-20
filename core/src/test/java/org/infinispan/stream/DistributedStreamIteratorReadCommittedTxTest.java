package org.infinispan.stream;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Test streams with READ_COMMITTED isolation level (reproducer for ISPN-9305)
 *
 * @author Dan Berindei
 * @since 9.3
 */
@Test(groups = {"functional", "smoke"}, testName = "stream.DistributedStreamIteratorReadCommittedTxTest")
public class DistributedStreamIteratorReadCommittedTxTest extends DistributedStreamIteratorRepeatableReadTxTest {
   @Override
   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
   }
}

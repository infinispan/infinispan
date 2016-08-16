package org.infinispan.tx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

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

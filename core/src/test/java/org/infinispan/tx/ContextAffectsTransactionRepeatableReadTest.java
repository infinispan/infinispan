package org.infinispan.tx;

import javax.transaction.RollbackException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.test.Exceptions;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * This test is to ensure that values in the context are properly counted for various cache operations
 *
 * @author wburns
 * @since 6.0
 */
@Test (groups = "functional", testName = "tx.ContextAffectsTransactionRepeatableReadTest")
public class ContextAffectsTransactionRepeatableReadTest extends ContextAffectsTransactionReadCommittedTest {
   public Object[] factory() {
      return new Object[] {
            new ContextAffectsTransactionRepeatableReadTest().withStorage(StorageType.BINARY),
            new ContextAffectsTransactionRepeatableReadTest().withStorage(StorageType.OBJECT),
            new ContextAffectsTransactionRepeatableReadTest().withStorage(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected void configure(ConfigurationBuilder builder) {
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
   }

   @Override
   protected void safeCommit(boolean throwWriteSkew) throws Exception {
      if (throwWriteSkew) {
         Exceptions.expectException(RollbackException.class, tm()::commit);
      } else {
         tm().commit();
      }
   }
}

package org.infinispan.tx;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import jakarta.transaction.RollbackException;

/**
 * This test is to ensure that values in the context are properly counted for various cache operations
 *
 * @author wburns
 * @since 6.0
 */
@Test (groups = "functional", testName = "tx.ContextAffectsTransactionRepeatableReadTest")
public class ContextAffectsTransactionRepeatableReadTest extends ContextAffectsTransactionReadCommittedTest {
   @Factory
   public Object[] factory() {
      return new Object[] {
            new ContextAffectsTransactionRepeatableReadTest().withStorage(StorageType.HEAP),
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

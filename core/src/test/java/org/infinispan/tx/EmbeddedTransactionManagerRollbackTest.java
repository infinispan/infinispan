package org.infinispan.tx;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "tx.EmbeddedTransactionManagerRollbackTest")
public class EmbeddedTransactionManagerRollbackTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.transaction().useSynchronization(true);
      builder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testCallerReceivesException() {
      Cache<String, String> c = cache();
      TestingUtil.extractInterceptorChain(c).addInterceptor(new FailInterceptor(), 2);

      // Verifies the caller receives the exception after the rollback.
      assertThatThrownBy(() -> c.putAsync("key", "value").get(10, TimeUnit.SECONDS))
            .hasMessageContaining("Oops");

      assertNoTransactions(c);
   }


   static class FailInterceptor extends DDAsyncInterceptor {
      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, throwable) -> {
            throw new RuntimeException("Oops");
         });
      }
   }
}

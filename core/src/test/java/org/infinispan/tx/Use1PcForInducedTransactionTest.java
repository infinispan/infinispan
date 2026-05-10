package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "tx.Use1PcForInducedTransactionTest")
public class Use1PcForInducedTransactionTest extends MultipleCacheManagersTest {

   private InvocationCountInterceptor ic0;
   private InvocationCountInterceptor ic1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      c.transaction().use1PcForAutoCommitTransactions(true);

      createCluster(c, 2);
      waitForClusterToForm();

      ic0 = new InvocationCountInterceptor();
      extractInterceptorChain(cache(0)).addInterceptor(ic0, 1);
      ic1 = new InvocationCountInterceptor();
      extractInterceptorChain(cache(1)).addInterceptor(ic1, 1);
   }

   public void testSinglePhaseCommit() {
      cache(0).put("k", "v");
      assertEquals("v", cache(0).get("k"));
      assertEquals("v", cache(1).get("k"));

      assertNotLocked("k");

      assertEquals(1, ic0.prepareInvocations);
      assertEquals(1, ic1.prepareInvocations);
      assertEquals(0, ic0.commitInvocations);
      assertEquals(0, ic0.commitInvocations);
   }


   public static class InvocationCountInterceptor extends DDAsyncInterceptor {

      volatile int prepareInvocations;
      volatile int commitInvocations;

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         prepareInvocations ++;
         return super.visitPrepareCommand(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         commitInvocations ++;
         return super.visitCommitCommand(ctx, command);
      }
   }
}

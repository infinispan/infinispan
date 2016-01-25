package org.infinispan.tx;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

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
      advancedCache(0).addInterceptor(ic0, 1);
      ic1 = new InvocationCountInterceptor();
      advancedCache(1).addInterceptor(ic1, 1);
   }

   public void testSinglePhaseCommit() {
      cache(0).put("k", "v");
      assert cache(0).get("k").equals("v");
      assert cache(1).get("k").equals("v");

      assertNotLocked("k");

      assertEquals(ic0.prepareInvocations.get(), 1);
      assertEquals(ic1.prepareInvocations.get(), 1);
      assertEquals(ic0.commitInvocations.get(), 0);
      assertEquals(ic0.commitInvocations.get(), 0);
   }


   public static class InvocationCountInterceptor extends CommandInterceptor {

      final AtomicInteger prepareInvocations = new AtomicInteger();
      final AtomicInteger commitInvocations = new AtomicInteger();

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         prepareInvocations.incrementAndGet();
         return super.visitPrepareCommand(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         commitInvocations.incrementAndGet();
         return super.visitCommitCommand(ctx, command);
      }
   }
}

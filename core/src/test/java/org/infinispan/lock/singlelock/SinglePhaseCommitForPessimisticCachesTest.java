package org.infinispan.lock.singlelock;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

/**
 * Single phase commit is used with pessimistic caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.SinglePhaseCommitForPessimisticCachesTest")
public class SinglePhaseCommitForPessimisticCachesTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      c
         .clustering().hash().numOwners(3)
         .transaction().lockingMode(LockingMode.PESSIMISTIC);
      createCluster(c, 3);
      waitForClusterToForm();
   }

   public void testSinglePhaseCommit() throws Exception {
      final Object k0_1 = getKeyForCache(0);
      final Object k0_2 = getKeyForCache(0);

      final List<Address> members = advancedCache(0).getRpcManager().getTransport().getMembers();
      assert advancedCache(0).getDistributionManager().locate(k0_1).containsAll(members);
      assert advancedCache(0).getDistributionManager().locate(k0_2).containsAll(members);
      TxCountInterceptor interceptor0 = new TxCountInterceptor();
      TxCountInterceptor interceptor1 = new TxCountInterceptor();
      advancedCache(0).addInterceptor(interceptor0, 1);
      advancedCache(1).addInterceptor(interceptor1, 2);

      tm(2).begin();
      cache(2).put(k0_1, "v");
      cache(2).put(k0_2, "v");
      tm(2).commit();


      assertEquals(interceptor0.lockCount.get(), 2);
      assertEquals(interceptor1.lockCount.get(), 2);
      assertEquals(interceptor0.prepareCount.get(), 1);
      assertEquals(interceptor1.prepareCount.get(), 1);
      assertEquals(interceptor0.commitCount.get(), 0);
      assertEquals(interceptor1.commitCount.get(), 0);
   }


   public static class TxCountInterceptor extends CommandInterceptor {

      public final AtomicInteger prepareCount = new AtomicInteger(0);
      public final AtomicInteger commitCount = new AtomicInteger(0);
      public final AtomicInteger lockCount = new AtomicInteger(0);;
      public final AtomicInteger putCount = new AtomicInteger(0);;
      public final AtomicInteger getCount = new AtomicInteger(0);;

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         prepareCount.incrementAndGet();
         return super.visitPrepareCommand(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         commitCount.incrementAndGet();
         return super.visitCommitCommand(ctx, command);
      }

      @Override
      public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
         lockCount.incrementAndGet();
         return super.visitLockControlCommand(ctx, command);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         putCount.incrementAndGet();
         return super.visitPutKeyValueCommand(ctx, command);
      }

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         getCount.incrementAndGet();
         return super.visitGetKeyValueCommand(ctx, command);
      }
   }

}

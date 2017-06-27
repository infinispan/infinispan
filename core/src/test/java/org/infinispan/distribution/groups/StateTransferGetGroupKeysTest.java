package org.infinispan.distribution.groups;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface during the state transfer.
 * <p/>
 * Note: no tests were added for {@link org.infinispan.AdvancedCache#removeGroup(String)} because internally it uses the
 * {@link org.infinispan.commands.write.RemoveCommand}. If the implementation changes, the new tests must be added to
 * this class.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.StateTransferGetGroupKeysTest")
public class StateTransferGetGroupKeysTest extends BaseUtilGroupTest {

   @Override
   public Object[] factory() {
      return new Object[] {
         new StateTransferGetGroupKeysTest(TestCacheFactory.PRIMARY_OWNER),
         new StateTransferGetGroupKeysTest(TestCacheFactory.BACKUP_OWNER),
         new StateTransferGetGroupKeysTest(TestCacheFactory.NON_OWNER),
         new StateTransferGetGroupKeysTest(TestCacheFactory.PRIMARY_OWNER).cacheMode(CacheMode.SCATTERED_SYNC),
         new StateTransferGetGroupKeysTest(TestCacheFactory.NON_OWNER).cacheMode(CacheMode.SCATTERED_SYNC),
      };
   }

   public StateTransferGetGroupKeysTest() {
      super(null);
   }

   protected StateTransferGetGroupKeysTest(TestCacheFactory factory) {
      super(factory);
   }

   public void testGetGroupKeysDuringPrimaryOwnerChange() throws TimeoutException, InterruptedException, ExecutionException {
      /*
       * it tests multiple scenarios
       * 1) when the ownership changes (when we execute the query in the primary owner and a new one is elected)
       * 2) when the topology changes but the ownership doesn't (when we execute the query on the backup owner)
       * 3) when the ownership changes and the query is executed in a non-owner
       */
      final TestCache testCache = createTestCacheAndReset(GROUP, this.caches());
      initCache(testCache.primaryOwner);
      final BlockCommandInterceptor interceptor = injectBlockCommandInterceptorIfAbsent(extractTargetCache(testCache));

      interceptor.open = false;
      Future<Map<GroupKey, String>> future = fork(new Callable<Map<GroupKey, String>>() {
         @Override
         public Map<GroupKey, String> call() throws Exception {
            return testCache.testCache.getGroup(GROUP);
         }
      });

      interceptor.awaitCommandBlock();

      addClusterEnabledCacheManager(createConfigurationBuilder());
      waitForClusterToForm();

      interceptor.unblockCommandAndOpen();

      Map<GroupKey, String> groupKeySet = future.get();
      Map<GroupKey, String> expectedGroupSet = createMap(0, 10);
      AssertJUnit.assertEquals(expectedGroupSet, groupKeySet);
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
      super.clearContent();
      //in case we need to add more tests
      if (cleanupAfterTest()) {
         while (getCacheManagers().size() > 3) {
            killMember(3);
         }
         while (getCacheManagers().size() < 3) {
            addClusterEnabledCacheManager(createConfigurationBuilder());
         }
         waitForClusterToForm();
      }
   }

   @Override
   protected final void resetCaches(List<Cache<GroupKey, String>> cacheList) {
      for (Cache cache : cacheList) {
         AsyncInterceptorChain chain = cache.getAdvancedCache().getAsyncInterceptorChain();
         BlockCommandInterceptor interceptor = chain.findInterceptorWithClass(BlockCommandInterceptor.class);
         if (interceptor != null) {
            interceptor.reset();
         }
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, createConfigurationBuilder());
   }

   private static BlockCommandInterceptor injectBlockCommandInterceptorIfAbsent(Cache<GroupKey, String> cache) {
      AsyncInterceptorChain chain = cache.getAdvancedCache().getAsyncInterceptorChain();
      BlockCommandInterceptor interceptor = chain.findInterceptorWithClass(BlockCommandInterceptor.class);
      if (interceptor == null) {
         interceptor = new BlockCommandInterceptor();
         EntryWrappingInterceptor ewi = chain.findInterceptorExtending(EntryWrappingInterceptor.class);
         assertNotNull(ewi);
         chain.addInterceptorAfter(interceptor, ewi.getClass());
      }
      interceptor.reset();
      return interceptor;
   }

   private ConfigurationBuilder createConfigurationBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, false);
      builder.clustering().stateTransfer().fetchInMemoryState(true);
      builder.clustering().hash().groups().enabled(true);
      builder.clustering().hash().numSegments(1);
      if (cacheMode.isScattered()) {
         builder.clustering().hash().consistentHashFactory(new CustomConsistentHashFactory(new BaseControlledConsistentHashFactory.ScatteredTrait(), cacheMode));
      } else {
         builder.clustering().hash().consistentHashFactory(new CustomConsistentHashFactory(new BaseControlledConsistentHashFactory.DefaultTrait(), cacheMode));
      }
      return builder;
   }


   private static class CustomConsistentHashFactory<CH extends ConsistentHash> extends BaseControlledConsistentHashFactory<CH> {
      private final CacheMode cacheMode;

      private CustomConsistentHashFactory(Trait<CH> trait, CacheMode cacheMode) {
         super(trait, 1);
         this.cacheMode = cacheMode;
      }

      @Override
      protected List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex) {
         if (cacheMode.isDistributed()) {
            assertEquals(2, numberOfOwners);
            if (members.size() == 1)
               return Arrays.asList(members.get(0));
            else if (members.size() == 2)
               return Arrays.asList(members.get(0), members.get(1));
            else
               return Arrays.asList(members.get(members.size() - 1), members.get(0));
         } else if (cacheMode.isScattered()) {
            return Collections.singletonList(members.get(0));
         } else {
            throw new IllegalStateException();
         }
      }
   }

   private static class BlockCommandInterceptor extends CommandInterceptor {

      private volatile CheckPoint checkPoint;
      private volatile boolean open;

      private BlockCommandInterceptor() {
         checkPoint = new CheckPoint();
      }

      @Override
      public Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
         if (!open) {
            checkPoint.trigger("before");
            checkPoint.awaitStrict("after", 30, TimeUnit.SECONDS);
         }
         return invokeNextInterceptor(ctx, command);
      }

      public final void awaitCommandBlock() throws TimeoutException, InterruptedException {
         checkPoint.awaitStrict("before", 30, TimeUnit.SECONDS);
      }

      public final void unblockCommand() {
         checkPoint.trigger("after");
      }

      public final void unblockCommandAndOpen() {
         open = true;
         checkPoint.trigger("after");
      }

      public final void reset() {
         open = true;
         checkPoint = new CheckPoint();
      }
   }

}

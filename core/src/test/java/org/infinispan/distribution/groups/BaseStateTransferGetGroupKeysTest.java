package org.infinispan.distribution.groups;

import org.infinispan.Cache;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.AssertJUnit.assertEquals;

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
@Test(groups = "functional")
public abstract class BaseStateTransferGetGroupKeysTest extends BaseUtilGroupTest {

   protected BaseStateTransferGetGroupKeysTest(TestCacheFactory factory) {
      super(factory);
   }

   public void testGetGroupKeysDuringPrimaryOwnerChange() throws TimeoutException, InterruptedException, ExecutionException {
      /*
       * it tests multiple scenarios
       * 1) when the ownership changes (when we execute the query in the primary owner and a new one is elected)
       * 2) when the topology changes but the ownership doesn't (when we execute the query on the backup owner)
       * 3) when the ownership changes and the query is executed in a non-owner
       */
      final TestCache testCache = createTestCacheAndReset(GROUP, this.<GroupKey, String>caches());
      initCache(testCache.primaryOwner);
      final BlockCommandInterceptor interceptor = injectBlockCommandInterceptorIfAbsent(extractTargetCache(testCache));

      interceptor.open.set(false);
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
         InterceptorChain chain = TestingUtil.extractComponent(cache, InterceptorChain.class);
         if (chain.containsInterceptorType(BlockCommandInterceptor.class)) {
            ((BlockCommandInterceptor) chain.getInterceptorsWithClass(BlockCommandInterceptor.class).get(0)).reset();
         }
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, createConfigurationBuilder());
   }

   private static BlockCommandInterceptor injectBlockCommandInterceptorIfAbsent(Cache<GroupKey, String> cache) {
      InterceptorChain chain = TestingUtil.extractComponent(cache, InterceptorChain.class);
      BlockCommandInterceptor interceptor;
      if (chain.containsInterceptorType(BlockCommandInterceptor.class)) {
         interceptor = (BlockCommandInterceptor) chain.getInterceptorsWithClass(BlockCommandInterceptor.class).get(0);
      } else {
         interceptor = new BlockCommandInterceptor();
         chain.addInterceptorAfter(interceptor, EntryWrappingInterceptor.class);
      }
      interceptor.reset();
      return interceptor;
   }

   private static ConfigurationBuilder createConfigurationBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().stateTransfer().fetchInMemoryState(true);
      builder.clustering().hash().groups().enabled(true);
      builder.clustering().hash().numOwners(2);
      builder.clustering().hash().numSegments(1);
      builder.clustering().hash().consistentHashFactory(new CustomConsistentHashFactory());
      return builder;
   }


   private static class CustomConsistentHashFactory extends BaseControlledConsistentHashFactory {
      private CustomConsistentHashFactory() {
         super(1);
      }

      @Override
      protected List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex) {
         assertEquals(2, numberOfOwners);
         if (members.size() == 1)
            return Arrays.asList(members.get(0));
         else if (members.size() == 2)
            return Arrays.asList(members.get(0), members.get(1));
         else
            return Arrays.asList(members.get(members.size() - 1), members.get(0));
      }
   }

   private static class BlockCommandInterceptor extends CommandInterceptor {

      private AtomicReference<CheckPoint> checkPoint = new AtomicReference<>(new CheckPoint());
      private AtomicBoolean open = new AtomicBoolean();

      private BlockCommandInterceptor() {
      }

      @Override
      public Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
         if (!open.get()) {
            CheckPoint checkPoint = this.checkPoint.get();
            checkPoint.trigger("before");
            checkPoint.awaitStrict("after", 30, TimeUnit.SECONDS);
         }
         return invokeNextInterceptor(ctx, command);
      }

      public final void awaitCommandBlock() throws TimeoutException, InterruptedException {
         checkPoint.get().awaitStrict("before", 30, TimeUnit.SECONDS);
      }

      public final void unblockCommand() {
         checkPoint.get().trigger("after");
      }

      public final void unblockCommandAndOpen() {
         open.set(true);
         checkPoint.get().trigger("after");
      }

      public final void reset() {
         open.set(true);
         checkPoint.set(new CheckPoint());
      }
   }

}

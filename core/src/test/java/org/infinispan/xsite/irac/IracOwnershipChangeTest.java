package org.infinispan.xsite.irac;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.NonTxIracLocalSiteInterceptor;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests topology change while write command is executing.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "xsite.irac.IracOwnershipChangeTest")
public class IracOwnershipChangeTest extends AbstractMultipleSitesTest {

   private final ControlledConsistentHashFactory<?> site0CHFactory = new ControlledConsistentHashFactory.Default(0, 1);
   private final ControlledConsistentHashFactory<?> site1CHFactory = new ControlledConsistentHashFactory.Default(0, 1);


   @Override
   protected int defaultNumberOfSites() {
      return 2;
   }

   @Override
   protected int defaultNumberOfNodes() {
      return 3;
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().hash()
            .numSegments(1)
            .numOwners(2)
            .consistentHashFactory(siteIndex == 0 ? site0CHFactory : site1CHFactory);
      builder.sites().addBackup()
            .site(siteName(siteIndex == 0 ? 1 : 0))
            .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      return builder;
   }

   @Override
   protected GlobalConfigurationBuilder defaultGlobalConfigurationForSite(int index) {
      var cfg = super.defaultGlobalConfigurationForSite(index);
      cfg.serialization().addContextInitializer(ControlledConsistentHashFactory.SCI.INSTANCE);
      return cfg;
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   public void createBeforeMethod() {
      super.createBeforeMethod();
      // reset consistent hash
      site0CHFactory.setOwnerIndexes(0, 1);
      site1CHFactory.setOwnerIndexes(0, 1);

      site0CHFactory.triggerRebalance(cache(0, 0));
      site1CHFactory.triggerRebalance(cache(1, 0));

      site(0).waitForClusterToForm(null);
      site(1).waitForClusterToForm(null);
   }

   public void testPrimaryOwnerLosesOwnership() throws InterruptedException, ExecutionException, TimeoutException {
      String key = "key-1";
      String value = "primary-loses-ownership";

      assertOwnership(key, 0, 1, 2);

      BlockingInterceptor interceptor = blockingInterceptor(0, 0);
      CommandBlocker blocker = interceptor.blockCommand();

      CompletableFuture<?> stage = cache(0, 0).putAsync(key, value);

      AssertJUnit.assertTrue(blocker.blocked.await(10, TimeUnit.SECONDS));

      site0CHFactory.setOwnerIndexes(1, 2);
      site0CHFactory.triggerRebalance(cache(0, 0));
      site(0).waitForClusterToForm(null);
      assertOwnership(key, 1, 2, 0);

      blocker.release();
      stage.get(10, TimeUnit.SECONDS);

      eventuallyAssertInAllSitesAndCaches(cache -> value.equals(cache.get(key)));
   }

   public void testBackupOwnerLosesOwnership() throws InterruptedException, ExecutionException, TimeoutException {
      String key = "key-2";
      String value = "backup-loses-ownership";

      assertOwnership(key, 0, 1, 2);

      BlockingInterceptor interceptor = blockingInterceptor(0, 1);
      CommandBlocker blocker = interceptor.blockCommand();

      CompletableFuture<?> stage = cache(0, 1).putAsync(key, value);

      AssertJUnit.assertTrue(blocker.blocked.await(10, TimeUnit.SECONDS));

      site0CHFactory.setOwnerIndexes(0, 2);
      site0CHFactory.triggerRebalance(cache(0, 0));
      site(0).waitForClusterToForm(null);
      assertOwnership(key, 0, 2, 1);

      blocker.release();
      stage.get(10, TimeUnit.SECONDS);

      eventuallyAssertInAllSitesAndCaches(cache -> value.equals(cache.get(key)));
   }

   public void testPrimaryChangesOwnershipWithBackup() throws InterruptedException, ExecutionException, TimeoutException {
      String key = "key-3";
      String value = "primary-backup-swap";

      assertOwnership(key, 0, 1, 2);

      BlockingInterceptor interceptor = blockingInterceptor(0, 0);
      CommandBlocker blocker = interceptor.blockCommand();

      CompletableFuture<?> stage = cache(0, 0).putAsync(key, value);

      AssertJUnit.assertTrue(blocker.blocked.await(10, TimeUnit.SECONDS));

      site0CHFactory.setOwnerIndexes(1, 0);
      site0CHFactory.triggerRebalance(cache(0, 0));
      site(0).waitForClusterToForm(null);
      assertOwnership(key, 1, 0, 2);

      blocker.release();
      stage.get(10, TimeUnit.SECONDS);

      eventuallyAssertInAllSitesAndCaches(cache -> value.equals(cache.get(key)));
   }

   public void testNonOwnerBecomesBackup() throws InterruptedException, ExecutionException, TimeoutException {
      String key = "key-4";
      String value = "non-owner-to-backup";

      assertOwnership(key, 0, 1, 2);

      BlockingInterceptor interceptor = blockingInterceptor(0, 2);
      CommandBlocker blocker = interceptor.blockCommand();

      CompletableFuture<?> stage = cache(0, 2).putAsync(key, value);

      AssertJUnit.assertTrue(blocker.blocked.await(10, TimeUnit.SECONDS));

      site0CHFactory.setOwnerIndexes(0, 2);
      site0CHFactory.triggerRebalance(cache(0, 0));
      site(0).waitForClusterToForm(null);
      assertOwnership(key, 0, 2, 1);

      blocker.release();
      stage.get(10, TimeUnit.SECONDS);

      eventuallyAssertInAllSitesAndCaches(cache -> value.equals(cache.get(key)));
   }

   public void testNonOwnerBecomesPrimary() throws InterruptedException, ExecutionException, TimeoutException {
      String key = "key-5";
      String value = "non-owner-to-backup";

      assertOwnership(key, 0, 1, 2);

      BlockingInterceptor interceptor = blockingInterceptor(0, 2);
      CommandBlocker blocker = interceptor.blockCommand();

      CompletableFuture<?> stage = cache(0, 2).putAsync(key, value);

      AssertJUnit.assertTrue(blocker.blocked.await(10, TimeUnit.SECONDS));

      site0CHFactory.setOwnerIndexes(2, 1);
      site0CHFactory.triggerRebalance(cache(0, 0));
      site(0).waitForClusterToForm(null);
      assertOwnership(key, 2, 1, 0);

      blocker.release();
      stage.get(10, TimeUnit.SECONDS);

      eventuallyAssertInAllSitesAndCaches(cache -> value.equals(cache.get(key)));
   }

   private LocalizedCacheTopology cacheTopology(int site, int index) {
      return TestingUtil.extractCacheTopology(cache(site, index));
   }

   private BlockingInterceptor blockingInterceptor(int site, int index) {
      AsyncInterceptorChain interceptorChain = TestingUtil.extractInterceptorChain(cache(site, index));
      BlockingInterceptor interceptor = interceptorChain.findInterceptorExtending(BlockingInterceptor.class);
      if (interceptor != null) {
         return interceptor;
      }
      interceptor = new BlockingInterceptor();
      AssertJUnit.assertTrue(interceptorChain.addInterceptorAfter(interceptor, NonTxIracLocalSiteInterceptor.class));
      return interceptor;
   }

   private void assertOwnership(String key, int primary, int backup, int nonOwner) {
      AssertJUnit.assertTrue(cacheTopology(0, primary).getDistribution(key).isPrimary());
      AssertJUnit.assertTrue(cacheTopology(0, backup).getDistribution(key).isWriteBackup());
      AssertJUnit.assertFalse(cacheTopology(0, nonOwner).getDistribution(key).isWriteOwner());
   }

   public static class BlockingInterceptor extends DDAsyncInterceptor {

      volatile CommandBlocker afterCompleted;

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         CommandBlocker blocker = afterCompleted;
         if (blocker == null || blocker.delay.isDone() || command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            log.tracef("Skipping command %s", command);
            return invokeNext(ctx, command);
         }
         return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, throwable) -> delayedValue(blocker.notifyBlocked(rCommand), rv, throwable));
      }

      CommandBlocker blockCommand() {
         CommandBlocker existing;
         CommandBlocker newBlocker = new CommandBlocker(new CountDownLatch(1), new CompletableFuture<>());
         synchronized (this) {
            existing = afterCompleted;
            afterCompleted = newBlocker;
         }
         if (existing != null) {
            existing.release();
         }
         return newBlocker;
      }
   }

   private static class CommandBlocker {
      final CountDownLatch blocked;
      final CompletableFuture<Void> delay;

      CommandBlocker(CountDownLatch blocked, CompletableFuture<Void> delay) {
         this.blocked = Objects.requireNonNull(blocked);
         this.delay = Objects.requireNonNull(delay);
      }

      CompletableFuture<Void> notifyBlocked(Object command) {
         log.tracef("Blocking command %s", command);
         blocked.countDown();
         return delay.thenRun(() -> log.tracef("Unblocking command %s", command));
      }

      void release() {
         blocked.countDown();
         delay.complete(null);
      }
   }
}

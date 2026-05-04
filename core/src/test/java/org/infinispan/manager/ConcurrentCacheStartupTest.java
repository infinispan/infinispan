package org.infinispan.manager;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "manager.ConcurrentCacheStartupTest")
@CleanupAfterMethod
public class ConcurrentCacheStartupTest extends MultipleCacheManagersTest {

   private static final String CACHE_A = "cacheA";
   private static final String CACHE_B = "cacheB";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = replicatedSyncConfig();
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      EmbeddedCacheManager cm1 = addClusterEnabledCacheManager(gcb, new ConfigurationBuilder());
      cm1.defineConfiguration(CACHE_A, cfg.build());
      cm1.defineConfiguration(CACHE_B, cfg.build());
      cm1.getCache(CACHE_A).put("key", "value");
      cm1.getCache(CACHE_B).put("key", "value");
      waitForClusterToForm(CACHE_A, CACHE_B);
   }

   public void testCacheManagerRunningBeforeUserCachesComplete() throws Exception {
      CountDownLatch started = new CountDownLatch(1);
      CountDownLatch proceed = new CountDownLatch(1);

      EmbeddedCacheManager cm2 = startSecondNodeWithDelay(CACHE_A, started, proceed);

      assertThat(started.await(30, TimeUnit.SECONDS))
            .as("State transfer should have started").isTrue();
      assertThat(cm2.getStatus()).isEqualTo(ComponentStatus.RUNNING);

      CacheStartupManager sm = startupManager(cm2);
      assertThat(sm.getState(CACHE_A)).isEqualTo(CacheStartupState.STARTING);

      proceed.countDown();
      eventually(() -> sm.getState(CACHE_A) == CacheStartupState.READY);

      assertThat(cm2.<String, String>getCache(CACHE_A).get("key")).isEqualTo("value");
   }

   public void testStopDuringStartup() throws Exception {
      CountDownLatch started = new CountDownLatch(1);
      CountDownLatch proceed = new CountDownLatch(1);

      EmbeddedCacheManager cm2 = startSecondNodeWithDelay(CACHE_A, started, proceed);

      assertThat(started.await(30, TimeUnit.SECONDS))
            .as("State transfer should have started").isTrue();

      cm2.stop();
      proceed.countDown();

      assertThat(cm2.getStatus()).isEqualTo(ComponentStatus.TERMINATED);
   }

   public void testGetAllStatesSnapshot() throws Exception {
      CountDownLatch started = new CountDownLatch(1);
      CountDownLatch proceed = new CountDownLatch(1);

      EmbeddedCacheManager cm2 = startSecondNodeWithDelay(CACHE_A, started, proceed);

      assertThat(started.await(30, TimeUnit.SECONDS))
            .as("State transfer should have started").isTrue();

      CacheStartupManager sm = startupManager(cm2);
      Map<String, CacheStartupState> states = sm.getAllStates();
      assertThat(states).isNotNull();
      assertThat(states.get(CACHE_A)).isEqualTo(CacheStartupState.STARTING);

      proceed.countDown();
      eventually(() -> sm.getAllStates().values().stream()
            .noneMatch(s -> s == CacheStartupState.STARTING));

      assertThat(sm.getState(CACHE_A)).isEqualTo(CacheStartupState.READY);
      assertThat(sm.getState(CACHE_B)).isEqualTo(CacheStartupState.READY);
   }

   public void testCacheServesRequestsWhileOthersStarting() throws Exception {
      CountDownLatch started = new CountDownLatch(1);
      CountDownLatch proceed = new CountDownLatch(1);

      EmbeddedCacheManager cm2 = startSecondNodeWithDelay(CACHE_A, started, proceed);

      assertThat(started.await(30, TimeUnit.SECONDS))
            .as("State transfer should have started").isTrue();

      Cache<String, String> cacheB = cm2.getCache(CACHE_B);
      cacheB.put("newKey", "newValue");
      assertThat(cacheB.get("newKey")).isEqualTo("newValue");
      assertThat(cacheB.get("key")).isEqualTo("value");

      proceed.countDown();
      eventually(() -> startupManager(cm2).getState(CACHE_A) == CacheStartupState.READY);
   }

   public void testGetCacheBlocksUntilReady() throws Exception {
      CountDownLatch started = new CountDownLatch(1);
      CountDownLatch proceed = new CountDownLatch(1);

      EmbeddedCacheManager cm2 = startSecondNodeWithDelay(CACHE_A, started, proceed);

      assertThat(started.await(30, TimeUnit.SECONDS))
            .as("State transfer should have started").isTrue();

      Future<Cache<String, String>> getCacheFuture = fork(() -> cm2.getCache(CACHE_A));

      assertThat(getCacheFuture.isDone()).isFalse();
      assertThat(cm2.getStatus()).isEqualTo(ComponentStatus.RUNNING);
      assertThat(cm2.cacheExists(CACHE_A)).isTrue();

      proceed.countDown();

      Cache<String, String> cacheA = getCacheFuture.get(30, TimeUnit.SECONDS);
      assertThat(cacheA).isNotNull();
      assertThat(cacheA.get("key")).isEqualTo("value");
      assertThat(startupManager(cm2).getState(CACHE_A)).isEqualTo(CacheStartupState.READY);
   }

   private EmbeddedCacheManager startSecondNodeWithDelay(String delayedCache,
         CountDownLatch started, CountDownLatch proceed) {
      ConfigurationBuilder cfg = replicatedSyncConfig();

      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      TestCacheManagerFactory.addInterceptor(gcb, delayedCache::equals,
            new StateTransferLatchInterceptor(started, proceed),
            TestCacheManagerFactory.InterceptorPosition.BEFORE, InvocationContextInterceptor.class);

      TransportFlags flags = new TransportFlags();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(false,
            gcb, new ConfigurationBuilder(), flags);
      amendCacheManagerBeforeStart(cm);
      cacheManagers.add(cm);

      cm.defineConfiguration(CACHE_A, cfg.build());
      cm.defineConfiguration(CACHE_B, cfg.build());

      CompletableFuture.runAsync(cm::start);

      return cm;
   }

   private static ConfigurationBuilder replicatedSyncConfig() {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cfg.clustering().stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(true);
      return cfg;
   }

   private static CacheStartupManager startupManager(EmbeddedCacheManager cm) {
      return GlobalComponentRegistry.of(cm).getComponent(CacheStartupManager.class);
   }

   static class StateTransferLatchInterceptor extends BaseAsyncInterceptor {
      private final CountDownLatch started;
      private final CountDownLatch proceed;

      StateTransferLatchInterceptor(CountDownLatch started, CountDownLatch proceed) {
         this.started = started;
         this.proceed = proceed;
      }

      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
         if (cmd instanceof PutKeyValueCommand put
               && put.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            started.countDown();
            if (!proceed.await(30, TimeUnit.SECONDS)) {
               throw new TimeoutException("State transfer proceed latch timed out");
            }
         }
         return invokeNext(ctx, cmd);
      }
   }
}

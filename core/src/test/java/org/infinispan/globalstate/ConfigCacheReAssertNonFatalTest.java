package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.testing.Testing.tmpDirectory;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "globalstate.ConfigCacheReAssertNonFatalTest")
public class ConfigCacheReAssertNonFatalTest extends AbstractInfinispanTest {

   private static final String CACHE_NAME = "reassertNonFatal";

   /**
    * A returning node that cannot re-assert a persisted cache into the CONFIG cache must still start,
    * with that cache defined and started locally from its persisted state.
    * <p>
    * The returning node joins a partner whose CONFIG cache is empty, so the skip-if-present check does
    * not apply and the node re-asserts the cache. The re-assert is a REPL_SYNC write, so a partner-side
    * failure for that cache's CONFIG key propagates back and fails the write. The node's startup must
    * tolerate the failure and keep the cache operational locally.
    */
   public void testStartupSurvivesConfigReAssertFailureAndCacheRunsLocally() throws Exception {
      String baseDir = tmpDirectory(this.getClass().getSimpleName());
      Util.recursiveFileRemove(baseDir);
      String stateA = tmpDirectory(this.getClass().getSimpleName(), "A");
      String stateB = tmpDirectory(this.getClass().getSimpleName(), "B");

      // Node A owns the cache, persisting it to disk and to CONFIG, then leaves the cluster.
      EmbeddedCacheManager cmA = TestCacheManagerFactory.createClusteredCacheManager(
            false, statefulGlobal(stateA, true, false), new ConfigurationBuilder(), new TransportFlags());
      cmA.start();
      try {
         cmA.administration().createCache(CACHE_NAME, replicatedConfig());
         assertThat(cmA.cacheExists(CACHE_NAME)).isTrue();
      } finally {
         cmA.stop();
      }

      // Node B starts fresh with an empty CONFIG cache; it never knew about the cache.
      EmbeddedCacheManager cmB = TestCacheManagerFactory.createClusteredCacheManager(
            false, statefulGlobal(stateB, true, false), new ConfigurationBuilder(), new TransportFlags());
      cmB.start();

      EmbeddedCacheManager cmARestarted = null;
      try {
         // Fail every replication of the cache's CONFIG entry onto node B. The re-assert is REPL_SYNC, so
         // the partner-side failure propagates back to the writer and fails the write.
         Cache<Object, Object> configCacheB = cmB.getCache(InternalCacheNames.CONFIG_STATE_CACHE_NAME);
         ScopedState blockedKey = new ScopedState(ScopeType.CACHE.toString(), CACHE_NAME);
         TestingUtil.extractInterceptorChain(configCacheB)
               .addInterceptor(new FailWriteForKeyInterceptor(blockedKey, Integer.MAX_VALUE), 0);

         // Node A returns and joins B, adopting B's empty CONFIG, then re-asserts the persisted cache.
         cmARestarted = TestCacheManagerFactory.createClusteredCacheManager(
               false, statefulGlobal(stateA, false, false), new ConfigurationBuilder(), new TransportFlags());
         cmARestarted.start();
         TestingUtil.blockUntilViewsReceived(30000, cmB, cmARestarted);

         // The failed re-assert did not abort startup, and the cache runs locally from persisted state.
         assertThat(cmARestarted.cacheExists(CACHE_NAME)).isTrue();
         Cache<String, String> cache = cmARestarted.getCache(CACHE_NAME);
         cache.put("k", "v");
         assertThat(cache.get("k")).isEqualTo("v");
      } finally {
         TestingUtil.killCacheManagers(cmARestarted, cmB);
         Util.recursiveFileRemove(baseDir);
      }
   }

   /**
    * A re-assert that fails on the first attempt is retried by a single catch-up pass and lands in the
    * cluster CONFIG.
    * <p>
    * The returning node re-joins a partner whose CONFIG cache rejects the first write for the cache's key
    * and accepts the next one, so the entry only reaches the cluster CONFIG if a catch-up pass retries the
    * failed write.
    */
   public void testFailedReAssertIsRetriedByCatchUpPass() throws Exception {
      String baseDir = tmpDirectory(this.getClass().getSimpleName(), "catchup");
      Util.recursiveFileRemove(baseDir);
      String stateReasserter = tmpDirectory(this.getClass().getSimpleName(), "catchup", "reasserter");
      String stateOwner = tmpDirectory(this.getClass().getSimpleName(), "catchup", "owner");

      // The re-asserting node owns the cache alone, persisting it to disk and to CONFIG, then leaves.
      EmbeddedCacheManager reasserter = TestCacheManagerFactory.createClusteredCacheManager(
            false, statefulGlobal(stateReasserter, true, false), new ConfigurationBuilder(), new TransportFlags());
      reasserter.start();
      try {
         reasserter.administration().createCache(CACHE_NAME, replicatedConfig());
         assertThat(reasserter.cacheExists(CACHE_NAME)).isTrue();
      } finally {
         reasserter.stop();
      }

      // The owner starts fresh with an empty CONFIG; its write for the cache's key fails once, then succeeds.
      EmbeddedCacheManager owner = TestCacheManagerFactory.createClusteredCacheManager(
            false, statefulGlobal(stateOwner, true, false), new ConfigurationBuilder(), new TransportFlags());
      owner.start();

      EmbeddedCacheManager reasserterRestarted = null;
      try {
         Cache<Object, Object> configCacheOwner = owner.getCache(InternalCacheNames.CONFIG_STATE_CACHE_NAME);
         ScopedState blockedKey = new ScopedState(ScopeType.CACHE.toString(), CACHE_NAME);
         TestingUtil.extractInterceptorChain(configCacheOwner)
               .addInterceptor(new FailWriteForKeyInterceptor(blockedKey, 1), 0);

         // The returning node re-joins and re-asserts the persisted cache.
         reasserterRestarted = TestCacheManagerFactory.createClusteredCacheManager(
               false, statefulGlobal(stateReasserter, false, false), new ConfigurationBuilder(), new TransportFlags());
         reasserterRestarted.start();
         TestingUtil.blockUntilViewsReceived(30000, owner, reasserterRestarted);

         // The catch-up pass retried the first, failed write, so the entry reached the cluster CONFIG.
         assertThat(configCacheOwner.containsKey(blockedKey)).isTrue();
         assertThat(owner.cacheConfigurationExists(CACHE_NAME)).isTrue();
      } finally {
         TestingUtil.killCacheManagers(reasserterRestarted, owner);
         Util.recursiveFileRemove(baseDir);
      }
   }

   private GlobalConfigurationBuilder statefulGlobal(String stateDirectory, boolean clear, boolean zeroCapacity) {
      if (clear) {
         Util.recursiveFileRemove(stateDirectory);
      }
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable()
            .persistentLocation(stateDirectory)
            .configurationStorage(ConfigurationStorage.OVERLAY);
      if (zeroCapacity) {
         global.zeroCapacityNode(true);
      }
      return global;
   }

   private static Configuration replicatedConfig() {
      return new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.REPL_SYNC)
            .build();
   }

   /**
    * Fails the first {@code failures} writes whose key matches the given CONFIG key, then lets writes
    * through. Other CONFIG writes always pass. Installed at the top of the chain so it fires whether the
    * node handles the key as primary or as backup.
    */
   static final class FailWriteForKeyInterceptor extends DDAsyncInterceptor {
      private final ScopedState blockedKey;
      private final AtomicInteger remainingFailures;

      FailWriteForKeyInterceptor(ScopedState blockedKey, int failures) {
         this.blockedKey = blockedKey;
         this.remainingFailures = new AtomicInteger(failures);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         if (blockedKey.equals(command.getKey()) && remainingFailures.getAndDecrement() > 0) {
            throw new CacheException("Injected CONFIG replication failure for " + blockedKey);
         }
         return invokeNext(ctx, command);
      }
   }
}

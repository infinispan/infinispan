package org.infinispan.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.persistence.file.SingleFileStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "manager.CacheStartupManagerTest")
public class CacheStartupManagerTest extends SingleCacheManagerTest {

   private final String tmpDirectory = Testing.tmpDirectory(this.getClass());

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager((ConfigurationBuilder) null);
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   private CacheStartupManager startupManager() {
      return GlobalComponentRegistry.of(cacheManager).getComponent(CacheStartupManager.class);
   }

   private static EmbeddedCacheManager createIsolatedCacheManager() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      return TestCacheManagerFactory.createCacheManager(gcb, null, false);
   }

   private static CacheStartupManager startupManager(EmbeddedCacheManager cm) {
      return GlobalComponentRegistry.of(cm).getComponent(CacheStartupManager.class);
   }

   public void testInternalCacheFailureIsFatal() {
      assertThatThrownBy(() -> startupManager().startInternalCaches(Set.of("nonExistentInternalCache")))
            .isInstanceOf(Exception.class);
   }

   public void testUserCacheFailureIsIsolated() {
      cacheManager.defineConfiguration("goodCache", new ConfigurationBuilder().build());

      ConfigurationBuilder brokenConfig = new ConfigurationBuilder();
      brokenConfig.persistence()
            .addStore(SingleFileStoreConfigurationBuilder.class)
            .location("/nonexistent/invalid/path/that/will/fail");
      cacheManager.defineConfiguration("brokenCache", brokenConfig.build());

      CacheStartupManager sm = startupManager();
      sm.startUserCaches(Set.of("goodCache", "brokenCache"));

      eventually(() -> sm.getState("goodCache") != CacheStartupState.STARTING
            && sm.getState("brokenCache") != CacheStartupState.STARTING);

      assertThat(sm.getState("goodCache")).isEqualTo(CacheStartupState.READY);
      assertThat(sm.getState("brokenCache")).isEqualTo(CacheStartupState.FAILED);
      assertThat(cacheManager.getStatus()).isEqualTo(ComponentStatus.RUNNING);
   }

   public void testFailedCacheDoesNotPreventOtherCachesFromStarting() {
      EmbeddedCacheManager cm = createIsolatedCacheManager();
      try {
         cm.defineConfiguration("alpha", new ConfigurationBuilder().build());
         cm.defineConfiguration("beta", new ConfigurationBuilder().build());
         cm.defineConfiguration("gamma", new ConfigurationBuilder().build());
         cm.defineConfiguration("delta", new ConfigurationBuilder().build());

         ConfigurationBuilder brokenConfig = new ConfigurationBuilder();
         brokenConfig.persistence()
               .addStore(SingleFileStoreConfigurationBuilder.class)
               .location("/nonexistent/invalid/path/that/will/fail");
         cm.defineConfiguration("broken", brokenConfig.build());

         cm.start();

         CacheStartupManager sm = startupManager(cm);

         assertThat(sm.getState("broken")).isEqualTo(CacheStartupState.FAILED);
         assertThat(sm.getState("alpha")).isEqualTo(CacheStartupState.READY);
         assertThat(sm.getState("beta")).isEqualTo(CacheStartupState.READY);
         assertThat(sm.getState("gamma")).isEqualTo(CacheStartupState.READY);
         assertThat(sm.getState("delta")).isEqualTo(CacheStartupState.READY);

         assertThat(cm.getStatus()).isEqualTo(ComponentStatus.RUNNING);

         assertThat(cm.isRunning("alpha")).isTrue();
         assertThat(cm.isRunning("beta")).isTrue();
         assertThat(cm.isRunning("gamma")).isTrue();
         assertThat(cm.isRunning("delta")).isTrue();
         assertThat(cm.isRunning("broken")).isFalse();

         cm.getCache("alpha").put("key", "value");
         assertThat((String) cm.getCache("alpha").get("key")).isEqualTo("value");
      } finally {
         cm.stop();
      }
   }

   public void testMultipleStartUserCachesCallsAccumulate() {
      cacheManager.defineConfiguration("cache1", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("cache2", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("cache3", new ConfigurationBuilder().build());

      CacheStartupManager sm = startupManager();
      sm.startUserCaches(Set.of("cache1", "cache2"));
      sm.startUserCaches(Set.of("cache3"));

      eventually(() -> sm.getAllStates().values().stream()
            .noneMatch(s -> s == CacheStartupState.STARTING));

      assertThat(sm.getState("cache1")).isEqualTo(CacheStartupState.READY);
      assertThat(sm.getState("cache2")).isEqualTo(CacheStartupState.READY);
      assertThat(sm.getState("cache3")).isEqualTo(CacheStartupState.READY);
      assertThat(sm.getAllStates()).hasSize(3).containsValue(CacheStartupState.READY);

      assertThat(cacheManager.getCache("cache1")).isNotNull();
      assertThat(cacheManager.getCache("cache2")).isNotNull();
      assertThat(cacheManager.getCache("cache3")).isNotNull();
   }

   public void testStateIsReadyAfterRetryingFailedCache() throws Exception {
      // A regular file where the store expects a directory makes the cache fail to start.
      Path location = Path.of(tmpDirectory, "retry-cache-store");
      Files.createDirectories(location.getParent());
      Files.deleteIfExists(location);
      Files.createFile(location);

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.persistence()
            .addStore(SingleFileStoreConfigurationBuilder.class)
            .location(location.toString());

      EmbeddedCacheManager cm = createIsolatedCacheManager();
      try {
         cm.defineConfiguration("retried", cfg.build());
         cm.start();

         CacheStartupManager sm = startupManager(cm);
         assertThat(sm.getState("retried")).isEqualTo(CacheStartupState.FAILED);

         // Removing the file allows the store, and thus the cache, to start.
         Files.delete(location);

         assertThat(cm.getCache("retried")).isNotNull();
         assertThat(sm.getState("retried")).isEqualTo(CacheStartupState.READY);
      } finally {
         cm.stop();
      }
   }

   public void testCachesStartedAfterTheContainerAreNotTracked() {
      EmbeddedCacheManager cm = createIsolatedCacheManager();
      try {
         cm.defineConfiguration("tracked", new ConfigurationBuilder().build());
         cm.start();

         CacheStartupManager sm = startupManager(cm);
         assertThat(sm.getState("tracked")).isEqualTo(CacheStartupState.READY);

         cm.defineConfiguration("runtimeCache", new ConfigurationBuilder().build());
         assertThat(cm.getCache("runtimeCache")).isNotNull();

         assertThat(sm.getState("runtimeCache")).isNull();
         assertThat(sm.getAllStates()).containsOnlyKeys("tracked");
      } finally {
         cm.stop();
      }
   }

   public void testGetAllStatesReturnsImmutableSnapshot() {
      EmbeddedCacheManager cm = createIsolatedCacheManager();
      try {
         cm.defineConfiguration("first", new ConfigurationBuilder().build());
         cm.start();

         CacheStartupManager sm = startupManager(cm);
         Map<String, CacheStartupState> snapshot = sm.getAllStates();
         assertThat(snapshot).containsOnlyKeys("first");

         cm.defineConfiguration("second", new ConfigurationBuilder().build());
         sm.startUserCaches(Set.of("second"));
         eventually(() -> sm.getState("second") == CacheStartupState.READY);

         assertThat(snapshot).containsOnlyKeys("first");
         assertThatThrownBy(() -> snapshot.put("third", CacheStartupState.READY))
               .isInstanceOf(UnsupportedOperationException.class);
      } finally {
         cm.stop();
      }
   }
}

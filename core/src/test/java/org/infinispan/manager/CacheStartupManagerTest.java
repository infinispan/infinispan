package org.infinispan.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.persistence.file.SingleFileStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "manager.CacheStartupManagerTest")
public class CacheStartupManagerTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager((ConfigurationBuilder) null);
   }

   private CacheStartupManager startupManager() {
      return GlobalComponentRegistry.of(cacheManager).getComponent(CacheStartupManager.class);
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
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gcb, null, false);
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

         CacheStartupManager sm = GlobalComponentRegistry.of(cm).getComponent(CacheStartupManager.class);

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
}

package org.infinispan.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;

import org.infinispan.configuration.cache.ConfigurationBuilder;
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

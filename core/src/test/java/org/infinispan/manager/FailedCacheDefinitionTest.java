package org.infinispan.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.lang.reflect.Method;
import java.util.concurrent.CompletionException;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.topology.CacheJoinException;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "manager.FailedCacheDefinitionTest")
public class FailedCacheDefinitionTest extends AbstractInfinispanTest {

   public void testJoinStatelessWithState(Method m) {
      String state1 = tmpDirectory(this.getClass().getSimpleName(), m.getName(), "1");
      String state2 = tmpDirectory(this.getClass().getSimpleName(), m.getName(), "2");

      GlobalConfigurationBuilder gcb1 = statefulGlobalBuilder(state1, true);
      GlobalConfigurationBuilder gcb2 = statefulGlobalBuilder(state2, true);

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(gcb1, null);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(gcb2, null);
      try {
         Configuration cacheConfig = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build();

         // Create cache and trigger shutdown
         Cache<?, ?> c1 = cm1.administration().getOrCreateCache(m.getName(), cacheConfig);
         Cache<?, ?> c2 = cm2.getCache(m.getName());
         assertThat(c1.getStatus().allowInvocations()).isTrue();
         assertThat(c2.getStatus().allowInvocations()).isTrue();
         TestingUtil.waitForNoRebalance(c1, c2);
         c1.shutdown();
         c2.shutdown();

         TestingUtil.killCacheManagers(cm1, cm2);

         // Start with a fresh state in CM1, and join with CM2.
         gcb1 = statefulGlobalBuilder(state1, true);
         cm1 =  TestCacheManagerFactory.createClusteredCacheManager(gcb1, null);

         // Since CM1 started fresh, it created the cache correctly.
         c1 =  cm1.administration().getOrCreateCache(m.getName(), cacheConfig);
         assertThat(c1.getStatus().allowInvocations()).isTrue();

         // Because CM2 tried to join with persistent state, the cache is in FAILED state.
         gcb2 = statefulGlobalBuilder(state2, false);
         final EmbeddedCacheManager cm2Final =  TestCacheManagerFactory.createClusteredCacheManager(gcb2, null);
         assertThatThrownBy(() -> cm2Final.getCache(m.getName()))
               .cause()
               .isInstanceOf(CompletionException.class)
               .hasMessageStartingWith(CacheJoinException.class.getName());

         // Ensure the manager is still running.
         assertThat(cm2Final.getStatus().allowInvocations()).isTrue();
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   private GlobalConfigurationBuilder statefulGlobalBuilder(String stateDirectory, boolean clear) {
      if (clear) Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory).sharedPersistentLocation(stateDirectory).configurationStorage(ConfigurationStorage.OVERLAY);
      return global;
   }
}

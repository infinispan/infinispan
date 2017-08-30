package org.infinispan.manager;

import static org.infinispan.commons.api.BasicCacheContainer.DEFAULT_CACHE_NAME;
import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "functional", testName = "manager.CacheDependencyTest")
@CleanupAfterMethod
public class CacheDependencyTest extends SingleCacheManagerTest {

   @Test
   public void testExplicitStop() {
      cacheManager.defineConfiguration("A", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("B", new ConfigurationBuilder().build());
      Cache<?, ?> cacheA = cacheManager.getCache("A");
      Cache<?, ?> cacheB = cacheManager.getCache("B");

      cacheManager.addCacheDependency("A", "B");

      cacheB.stop();
      cacheA.stop();

      assertAllTerminated(cacheA, cacheB);
   }

   @Test
   public void testDependencyOnStoppedCaches() {
      cacheManager.defineConfiguration("A", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("B", new ConfigurationBuilder().build());
      Cache<?, ?> cacheA = cacheManager.getCache("A");
      Cache<?, ?> cacheB = cacheManager.getCache("B");
      cacheA.stop();

      cacheManager.addCacheDependency("A", "B");

      CacheEventListener listener = new CacheEventListener();
      cacheManager.addListener(listener);

      cacheManager.stop();

      assertAllTerminated(cacheA, cacheB);
      assertEquals(Arrays.asList("B", GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME, DEFAULT_CACHE_NAME), listener.stopOrder);
   }

   @Test
   public void testCyclicDependencies() {
      cacheManager.defineConfiguration("A", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("B", new ConfigurationBuilder().build());
      Cache<?, ?> cacheA = cacheManager.getCache("A");
      Cache<?, ?> cacheB = cacheManager.getCache("B");

      cacheManager.addCacheDependency("A", "B");
      cacheManager.addCacheDependency("B", "A");

      // Order will not be enforced
      cacheManager.stop();

      assertAllTerminated(cacheA, cacheB);
   }

   @Test
   public void testStopCacheManager() {
      CacheEventListener listener = new CacheEventListener();
      cacheManager.addListener(listener);

      cacheManager.defineConfiguration("A", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("B", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("C", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("D", new ConfigurationBuilder().build());

      Cache<?, ?> cacheA = cacheManager.getCache("A");
      Cache<?, ?> cacheB = cacheManager.getCache("B");
      Cache<?, ?> cacheC = cacheManager.getCache("C");
      Cache<?, ?> cacheD = cacheManager.getCache("D");

      cacheManager.addCacheDependency("A", "B");
      cacheManager.addCacheDependency("A", "C");
      cacheManager.addCacheDependency("A", "D");
      cacheManager.addCacheDependency("B", "C");
      cacheManager.addCacheDependency("B", "D");
      cacheManager.addCacheDependency("D", "C");

      cacheManager.stop();

      assertAllTerminated(cacheA, cacheB, cacheC, cacheD);
      assertEquals(Arrays.asList("A", "B", "D", "C", GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME, DEFAULT_CACHE_NAME), listener.stopOrder);
   }

   @Test
   public void testRemoveCache() {
      cacheManager.defineConfiguration("A", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("B", new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("C", new ConfigurationBuilder().build());

      Cache<?, ?> cacheA = cacheManager.getCache("A");
      Cache<?, ?> cacheB = cacheManager.getCache("B");
      Cache<?, ?> cacheC = cacheManager.getCache("C");

      cacheManager.addCacheDependency("A", "B");
      cacheManager.addCacheDependency("A", "C");
      cacheManager.addCacheDependency("B", "C");

      cacheManager.removeCache("B");

      CacheEventListener listener = new CacheEventListener();
      cacheManager.addListener(listener);

      cacheManager.stop();

      assertAllTerminated(cacheA, cacheB, cacheC);
      assertEquals(Arrays.asList("A", "C", GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME, DEFAULT_CACHE_NAME), listener.stopOrder);

   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Listener
   private static final class CacheEventListener {
      private final List<String> stopOrder = new ArrayList<>();

      @CacheStopped
      public void cacheStopped(CacheStoppedEvent cse) {
         stopOrder.add(cse.getCacheName());
      }
   }

   private void assertAllTerminated(Cache<?, ?>... caches) {
      for (Cache<?, ?> cache : caches) {
         assert cache.getStatus() == ComponentStatus.TERMINATED;
      }
   }
}

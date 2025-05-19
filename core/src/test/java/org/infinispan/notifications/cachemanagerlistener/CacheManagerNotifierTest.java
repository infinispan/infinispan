package org.infinispan.notifications.cachemanagerlistener;

import static org.testng.AssertJUnit.assertNotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "notifications.cachemanagerlistener.CacheManagerNotifierTest")
public class CacheManagerNotifierTest extends AbstractInfinispanTest {

   public void testViewChange() throws Exception {
      EmbeddedCacheManager cmA = TestCacheManagerFactory.createClusteredCacheManager();
      CacheContainer cmB = null;
      try {
         cmA.getCache();

         GetCacheManagerCheckListener listener = new GetCacheManagerCheckListener();
         cmA.addListener(listener);

         cmB = TestCacheManagerFactory.createClusteredCacheManager();
         cmB.getCache();

         assertNotNull(listener.firstEvent.get(10, TimeUnit.SECONDS));
      } finally {
         TestingUtil.killCacheManagers(cmA, cmB);
      }
   }

   public void testAddRemoveListenerWhileNotRunning() throws Exception {
      GetCacheManagerCheckListener listener = new GetCacheManagerCheckListener();
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.getGlobalConfigurationBuilder().clusteredDefault().defaultCacheName("default");
      holder.newConfigurationBuilder("default").clustering().cacheMode(CacheMode.DIST_SYNC);
      EmbeddedCacheManager cmA = TestCacheManagerFactory.createClusteredCacheManager(false, holder);
      EmbeddedCacheManager cmB = null;

      try {
         cmA.addListener(listener);

         cmA.getCache();

         cmB = TestCacheManagerFactory.createClusteredCacheManager();
         cmB.getCache();

         assertNotNull(listener.firstEvent.get(10, TimeUnit.SECONDS));
      } finally {
         TestingUtil.killCacheManagers(cmA, cmB);
      }

      // Should not throw an exception?
      cmA.removeListener(listener);
   }

   @Listener
   public static class GetCacheManagerCheckListener {
      CompletableFuture<ViewChangedEvent> firstEvent = new CompletableFuture<>();

      @ViewChanged
      public void onViewChange(ViewChangedEvent e) {
         firstEvent.complete(e);
      }
   }
}

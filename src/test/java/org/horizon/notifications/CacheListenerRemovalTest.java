package org.horizon.notifications;

import org.horizon.Cache;
import org.horizon.test.fwk.TestCacheManagerFactory;
import org.horizon.manager.CacheManager;
import org.horizon.notifications.cachelistener.annotation.CacheEntryVisited;
import org.horizon.notifications.cachelistener.event.Event;
import org.horizon.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "notifications.CacheListenerRemovalTest")
public class CacheListenerRemovalTest {
   Cache<String, String> cache;
   CacheManager cm;

   @BeforeMethod
   public void setUp() {
      cm = TestCacheManagerFactory.createLocalCacheManager();
      cache = cm.getCache();
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
   }

   public void testListenerRemoval() {
      cache.put("x", "y");
      AtomicInteger i = new AtomicInteger(0);
      assert 0 == cache.getListeners().size();
      CacheListener l = new CacheListener(i);
      cache.addListener(l);
      assert 1 == cache.getListeners().size();
      assert cache.getListeners().iterator().next() == l;
      assert 0 == i.get();
      cache.get("x");
      assert 1 == i.get();

      // remove the replListener
      cache.removeListener(l);
      assert 0 == cache.getListeners().size();
      i.set(0);
      assert 0 == i.get();
      cache.get("x");
      assert 0 == i.get();
   }

   @Listener
   public static class CacheListener {
      AtomicInteger i;

      private CacheListener(AtomicInteger i) {
         this.i = i;
      }

      @CacheEntryVisited
      public void listen(Event e) {
         if (e.isPre()) i.incrementAndGet();
      }
   }
}

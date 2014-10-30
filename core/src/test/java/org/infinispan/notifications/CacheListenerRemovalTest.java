package org.infinispan.notifications;

import org.infinispan.Cache;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.manager.CacheContainer;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "notifications.CacheListenerRemovalTest")
public class CacheListenerRemovalTest extends AbstractInfinispanTest {
   Cache<String, String> cache;
   CacheContainer cm;

   @BeforeMethod
   public void setUp() {
      cm = TestCacheManagerFactory.createCacheManager(false);
      cache = cm.getCache();
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
      cache = null;
   }

   public void testListenerRemoval() {
      cache.put("x", "y");
      AtomicInteger i = new AtomicInteger(0);
      int listenerSize = cache.getListeners().size();
      CacheListener l = new CacheListener(i);
      cache.addListener(l);
      assertEquals(listenerSize + 1, cache.getListeners().size());
      assert cache.getListeners().contains(l);
      assert 0 == i.get();
      cache.get("x");
      assert 1 == i.get();

      // remove the replListener
      cache.removeListener(l);
      assertEquals(listenerSize, cache.getListeners().size());
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

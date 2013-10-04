package org.infinispan.notifications.cachelistener;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.*;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.testng.AssertJUnit.*;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.TransactionSuspendedCacheNotifierTest")
@CleanupAfterMethod
public class TransactionSuspendedCacheNotifierTest extends SingleCacheManagerTest {

   public void testTransactionSuspended() throws Exception {
      TestListener listener = new TestListener();
      cache.getAdvancedCache().addListener(listener);

      assertTrue(cache.isEmpty());
      //created
      cache.put("key", "value");
      assertEquals("value", cache.get("key"));

      //modified
      cache.put("key", "new-value");
      assertEquals("new-value", cache.get("key"));

      tm().begin();
      assertEquals("new-value", cache.get("key"));
      tm().commit();

      //removed
      cache.remove("key");
      assertNull(cache.get("key"));

      cache.clear();
      assertTrue(cache.isEmpty());

      if (listener.list.size() > 0) {
         for (Throwable throwable : listener.list) {
            log.error("Error in listener...", throwable);
         }
         fail("Listener catch some errors");
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @Listener(sync = true)
   public static class TestListener {

      private final Log log = LogFactory.getLog(TestListener.class);
      private final List<Throwable> list = Collections.synchronizedList(new ArrayList<Throwable>(2));

      @CacheEntryActivated
      @CacheEntryCreated
      @CacheEntriesEvicted
      @CacheEntryInvalidated
      @CacheEntryLoaded
      @CacheEntryModified
      @CacheEntryPassivated
      @CacheEntryRemoved
      @CacheEntryVisited
      @TransactionCompleted
      @TransactionRegistered
      public void handle(Event e) {
         try {
            Object value = e.getCache().getAdvancedCache().withFlags(Flag.SKIP_LISTENER_NOTIFICATION).get("key");
            log.debugf("Event=%s, value=%s", e, value);
         } catch (Throwable throwable) {
            list.add(throwable);
         }
      }

   }
}

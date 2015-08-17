package org.infinispan.expiration.impl;

import org.infinispan.expiration.ExpirationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.*;

@Test(groups = "functional", testName = "expiration.impl.ExpirationStoreFunctionalTest")
public class ExpirationStoreListenerFunctionalTest extends ExpirationStoreFunctionalTest {
   protected ExpiredCacheListener listener = new ExpiredCacheListener();
   protected ExpirationManager manager;

   @Override
   protected void afterCacheCreated(EmbeddedCacheManager cm) {
      cache.addListener(listener);
      manager = TestingUtil.extractComponent(cache, ExpirationManager.class);
   }

   @AfterMethod
   public void resetListener() {
      listener.reset();
   }

   @Override
   public void testSimpleExpirationLifespan() throws Exception {
      super.testSimpleExpirationLifespan();
      manager.processExpiration();
      assertExpiredEvents(SIZE);
   }

   @Override
   public void testSimpleExpirationMaxIdle() throws Exception {
      super.testSimpleExpirationMaxIdle();
      manager.processExpiration();
      assertExpiredEvents(SIZE);
   }

   public void testExpirationOfStoreWhenDataNotInMemory() throws Exception {
      String key = "k";
      cache.put(key, "v", 10, TimeUnit.MILLISECONDS);
      cache.getAdvancedCache().getDataContainer().remove(key);
      // At this point data should only be in store - we assume size won't ressurect value into memory
      assertEquals(1, cache.size());
      assertEquals(0, listener.getInvocationCount());
      timeService.advance(11);
      assertNull(cache.get(key));
      // Stores do not expire entries on load, thus we need to purge them
      manager.processExpiration();

      assertEquals(1, listener.getInvocationCount());

      CacheEntryExpiredEvent event = listener.events.iterator().next();
      assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, event.getType());
      assertEquals(cache, event.getCache());
      assertFalse(event.isPre());
      assertNotNull(event.getKey());
      // Unfortunately we don't guarantee value or metadata from store
      assertNull(event.getValue());
      assertNull(event.getMetadata());
   }

   private void assertExpiredEvents(int count) {
      assertEquals(count, listener.getInvocationCount());
      listener.getEvents().forEach(event -> {
         assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, event.getType());
         assertEquals(cache, event.getCache());
         assertFalse(event.isPre());
         assertNotNull(event.getKey());
         assertNotNull(event.getValue());
         assertNotNull(event.getMetadata());
      });
   }
}

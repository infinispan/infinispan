package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.persistence.spi.AdvancedCacheExpirationWriter;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationStoreListenerFunctionalTest")
public class ExpirationStoreListenerFunctionalTest extends ExpirationStoreFunctionalTest {
   protected ExpiredCacheListener listener = new ExpiredCacheListener();

   @Factory
   @Override
   public Object[] factory() {
      return new Object[]{
            // Test is for dummy store with a listener and we don't care about memory storage types
            new ExpirationStoreListenerFunctionalTest().cacheMode(CacheMode.LOCAL),
      };
   }

   @Override
   protected String parameters() {
      return null;
   }

   @Override
   protected void afterCacheCreated(EmbeddedCacheManager cm) {
      cache.addListener(listener);
   }

   @AfterMethod
   public void resetListener() {
      listener.reset();
   }

   @Override
   public void testSimpleExpirationLifespan() throws Exception {
      super.testSimpleExpirationLifespan();
      expirationManager.processExpiration();
      assertExpiredEvents(SIZE);
   }

   @Override
   public void testSimpleExpirationMaxIdle() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i,-1, null, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);
      // We have to process expiration for store and max idle
      expirationManager.processExpiration();
      assertEquals(0, cache.size());
      assertExpiredEvents(SIZE);
   }

   public void testExpirationOfStoreWhenDataNotInMemory() throws Exception {
      String key = "k";
      cache.put(key, "v", 10, TimeUnit.MILLISECONDS);
      removeFromContainer(key);
      // At this point data should only be in store - we assume size won't ressurect value into memory
      assertEquals(1, cache.size());
      assertEquals(0, listener.getInvocationCount());
      timeService.advance(11);
      assertNull(cache.get(key));
      // Stores do not expire entries on load, thus we need to purge them
      expirationManager.processExpiration();

      assertEquals(1, listener.getInvocationCount());

      CacheEntryExpiredEvent event = listener.getEvents().iterator().next();
      assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, event.getType());
      assertEquals(cache, event.getCache());
      assertFalse(event.isPre());
      assertNotNull(event.getKey());
      // The dummy store produces value and metadata so lets make sure
      if (TestingUtil.getCacheLoader(cache) instanceof AdvancedCacheExpirationWriter) {
         assertEquals("v", event.getValue());
         assertNotNull(event.getMetadata());
      }
   }

   protected void removeFromContainer(String key) {
      cache.getAdvancedCache().getDataContainer().remove(key);
   }

   private void assertExpiredEvents(int count) {
      eventuallyEquals(count, () -> listener.getInvocationCount());
      listener.getEvents().forEach(event -> {
         log.tracef("Checking event %s", event);
         assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, event.getType());
         assertEquals(cache, event.getCache());
         assertFalse(event.isPre());
         assertNotNull(event.getKey());
         assertNotNull(event.getValue());
         assertNotNull(event.getMetadata());
      });
   }
}

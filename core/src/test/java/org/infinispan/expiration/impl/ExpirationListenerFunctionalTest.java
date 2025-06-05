package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.event.Event;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationListenerFunctionalTest")
public class ExpirationListenerFunctionalTest extends ExpirationFunctionalTest {

   protected ExpiredCacheListener listener = new ExpiredCacheListener();
   protected ExpirationManager manager;

   @Factory
   @Override
   public Object[] factory() {
      return new Object[]{
            new ExpirationListenerFunctionalTest().cacheMode(CacheMode.LOCAL).withStorage(StorageType.HEAP),
            new ExpirationListenerFunctionalTest().cacheMode(CacheMode.LOCAL).withStorage(StorageType.OFF_HEAP),
            new ExpirationListenerFunctionalTest().cacheMode(CacheMode.DIST_SYNC).withStorage(StorageType.HEAP),
            new ExpirationListenerFunctionalTest().cacheMode(CacheMode.DIST_SYNC).withStorage(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected void afterCacheCreated(EmbeddedCacheManager cm) {
      cache.addListener(listener);
      manager = cache.getAdvancedCache().getExpirationManager();
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

   public void testExpiredEventBetweenCreateEvent() {
      cache.put("foo", "bar", 1, TimeUnit.SECONDS);
      timeService.advance(2000);
      cache.put("foo", "bar2");
      assertExpiredEvents(1);
   }
}

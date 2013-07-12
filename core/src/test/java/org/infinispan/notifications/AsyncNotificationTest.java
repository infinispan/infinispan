package org.infinispan.notifications;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

@Test(groups = "functional", testName = "notifications.AsyncNotificationTest")
public class AsyncNotificationTest extends AbstractInfinispanTest {
   Cache<String, String> c;
   EmbeddedCacheManager cm;

   @BeforeMethod
   public void setUp() {
      cm = TestCacheManagerFactory.createCacheManager(false);
      c = cm.getCache();
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
      c = null;
   }

   public void testAsyncNotification() throws InterruptedException {
      CountDownLatch latch = new CountDownLatch(2);
      AbstractListener syncListener = new SyncListener(latch);
      AbstractListener asyncListener = new AsyncListener(latch);
      c.addListener(syncListener);
      c.addListener(asyncListener);
      c.put("k", "v");
      latch.await();
      assert syncListener.caller == Thread.currentThread();
      assert asyncListener.caller != Thread.currentThread();
   }

   public abstract static class AbstractListener {
      Thread caller;
      CountDownLatch latch;

      protected AbstractListener(CountDownLatch latch) {
         this.latch = latch;
      }
   }

   @Listener(sync = true)
   public static class SyncListener extends AbstractListener {
      public SyncListener(CountDownLatch latch) {
         super(latch);
      }

      @CacheEntryCreated
      public void handle(CacheEntryCreatedEvent e) {
         if (e.isPre()) {
            caller = Thread.currentThread();
            latch.countDown();
         }
      }
   }

   @Listener(sync = false)
   public static class AsyncListener extends AbstractListener {
      public AsyncListener(CountDownLatch latch) {
         super(latch);
      }

      @CacheEntryCreated
      public void handle(CacheEntryCreatedEvent e) {
         if (e.isPre()) {
            caller = Thread.currentThread();
            latch.countDown();
         }
      }
   }

}

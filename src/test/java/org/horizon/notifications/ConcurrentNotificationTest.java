package org.horizon.notifications;

import org.horizon.Cache;
import org.horizon.test.fwk.TestCacheManagerFactory;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.manager.CacheManager;
import org.horizon.notifications.cachelistener.annotation.CacheEntryCreated;
import org.horizon.notifications.cachelistener.annotation.CacheEntryModified;
import org.horizon.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.horizon.notifications.cachelistener.annotation.CacheEntryVisited;
import org.horizon.notifications.cachelistener.event.Event;
import org.horizon.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Test(groups = "functional", testName = "notifications.ConcurrentNotificationTest")
public class ConcurrentNotificationTest {
   Cache<String, String> cache;
   CacheManager cm;
   CacheListener listener;
   Log log = LogFactory.getLog(ConcurrentNotificationTest.class);

   @BeforeMethod
   public void setUp() {
      cm = TestCacheManagerFactory.createLocalCacheManager();
      cache = cm.getCache();
      listener = new CacheListener();
      cache.addListener(listener);
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
   }

   public void testThreads() throws Exception {
      Thread workers[] = new Thread[20];
      final List<Exception> exceptions = new LinkedList<Exception>();
      final int loops = 100;
      final CountDownLatch latch = new CountDownLatch(1);

      for (int i = 0; i < workers.length; i++) {
         workers[i] = new Thread() {
            public void run() {
               try {
                  latch.await();
               }
               catch (InterruptedException e) {
               }

               for (int j = 0; j < loops; j++) {
                  try {
                     cache.put("key", "value");
                  }
                  catch (Exception e) {
                     exceptions.add(new Exception("Caused on thread " + getName() + " in loop " + j + " when doing a put()", e));
                  }

                  try {
                     cache.remove("key");
                  }
                  catch (Exception e) {
                     exceptions.add(new Exception("Caused on thread " + getName() + " in loop " + j + " when doing a remove()", e));
                  }

                  try {
                     cache.get("key");
                  }
                  catch (Exception e) {
                     exceptions.add(new Exception("Caused on thread " + getName() + " in loop " + j + " when doing a get()", e));
                  }
               }
            }
         };

         workers[i].start();
      }

      latch.countDown();

      for (Thread t : workers)
         t.join();

      for (Exception e : exceptions)
         throw e;

      // we cannot ascertain the exact number of invocations on the replListener since some removes would mean that other
      // gets would miss.  And this would cause no notification to fire for that get.  And we cannot be sure of the
      // timing between removes and gets, so we just make sure *some* of these have got through, and no exceptions
      // were thrown due to concurrent access.
      assert loops * workers.length < listener.counter.get();
   }

   @Listener
   public class CacheListener {
      private AtomicInteger counter = new AtomicInteger(0);

      @CacheEntryModified
      @CacheEntryRemoved
      @CacheEntryVisited
      @CacheEntryCreated
      public void catchEvent(Event e) {
         if (e.isPre())
            counter.getAndIncrement();
      }
   }
}

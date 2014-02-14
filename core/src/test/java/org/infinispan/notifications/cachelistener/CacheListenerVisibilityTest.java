package org.infinispan.notifications.cachelistener;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryEvicted;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.AssertJUnit.*;

/**
 * Tests visibility of effects of cache operations on a separate thread once
 * a cache listener event has been consumed for the corresponding cache
 * operation.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "notifications.cachelistener.CacheListenerVisibilityTest")
@CleanupAfterMethod
public class CacheListenerVisibilityTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(false);
   }

   public void testSizeVisibility() throws Exception {
      updateCache(Visibility.SIZE);
   }

   public void testGetVisibility() throws Exception {
      updateCache(Visibility.GET);
   }

   public void testGetVisibilityWithinEntryCreatedListener() throws Exception {
      updateCacheAssertInListener(
            new EntryCreatedWithAssertListener(new CountDownLatch(1)));
   }

   public void testGetVisibilityWithinEntryModifiedListener() throws Exception {
      updateCacheAssertInListener(
            new EntryModifiedWithAssertListener(new CountDownLatch(1)));
   }

   public void testRemoveVisibility() throws Exception {
      cache.put(1, "v1");

      final CountDownLatch after = new CountDownLatch(1);
      final CountDownLatch afterContinue = new CountDownLatch(1);
      final CountDownLatch before = new CountDownLatch(1);
      cache.addListener(new EntryListener(before, afterContinue, after));

      assertEquals("v1", cache.get(1));

      Future<Void> ignore = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.remove(1);
            return null;
         }
      });

      // With removes, there's a before/after event acknowledgment, so verify it
      // Evicts on the other hand only emit a single event, after
      boolean signalled = before.await(30, TimeUnit.SECONDS);
      assertTrue("Timed out while waiting for before listener notification",
            signalled);

      assertEquals("v1", cache.get(1));

      // Let the isPre=false callback continue
      afterContinue.countDown();

      signalled = after.await(30, TimeUnit.SECONDS);
      assertTrue("Timed out while waiting for after listener notification",
            signalled);

      assertEquals(null, cache.get(1));

      ignore.get(5, TimeUnit.SECONDS);
   }

   public void testEvictOnCacheEntryEvictedVisibility() throws Exception {
      checkEvictVisibility(false);
   }

   public void testEvictOnCacheEntriesEvictedVisibility() throws Exception {
      checkEvictVisibility(true);
   }

   private void checkEvictVisibility(boolean isCacheEntriesEvicted) throws Exception {
      cache.put(1, "v1");

      final CountDownLatch after = new CountDownLatch(1);
      Object listener = isCacheEntriesEvicted
            ? new EntriesEvictedListener(after)
            : new EntryListener(null, null, after);

      cache.addListener(listener);
      assertEquals("v1", cache.get(1));

      Future<Void> ignore = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.evict(1);
            return null;
         }
      });

      boolean signalled = after.await(30, TimeUnit.SECONDS);
      assertTrue("Timed out while waiting for after listener notification",
            signalled);

      assertEquals(null, cache.get(1));

      ignore.get(5, TimeUnit.SECONDS);
   }

   public void testClearVisibility() throws Exception {
      cache.put(1, "v1");
      cache.put(2, "v1");
      cache.put(3, "v1");

      final CyclicBarrier after = new CyclicBarrier(2);
      final CountDownLatch afterContinue = new CountDownLatch(1);
      final CountDownLatch before = new CountDownLatch(1);
      cache.addListener(new CacheClearListener(before, afterContinue, after));

      assertEquals("v1", cache.get(1));
      assertEquals("v1", cache.get(2));
      assertEquals("v1", cache.get(3));

      Future<Void> ignore = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.clear();
            return null;
         }
      });

      boolean signalled = before.await(30, TimeUnit.SECONDS);
      assertTrue("Timed out while waiting for before listener notification",
            signalled);

      assertEquals("v1", cache.get(1));
      assertEquals("v1", cache.get(2));
      assertEquals("v1", cache.get(3));

      // Let the isPre=false callback continue
      afterContinue.countDown();

      // Wait for isPre=false remove notification for k=1
      after.await(30, TimeUnit.SECONDS);
      assertEquals(null, cache.get(1));

      // Wait for isPre=false remove notification for k=2
      after.await(30, TimeUnit.SECONDS);
      assertEquals(null, cache.get(2));

      // Wait for isPre=false remove notification for k=3
      after.await(30, TimeUnit.SECONDS);
      assertEquals(null, cache.get(3));

      assertTrue(cache.isEmpty());
      ignore.get(5, TimeUnit.SECONDS);
   }

   private void updateCacheAssertInListener(WithAssertListener listener)
         throws Exception {
      cache.addListener(listener);

      Future<Void> ignore = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.put("k", "v");
            return null;
         }
      });

      listener.latch.await(30, TimeUnit.SECONDS);
      assert listener.assertNotNull;
      assert listener.assertValue;
      ignore.get(5, TimeUnit.SECONDS);
   }

   private void updateCache(Visibility visibility) throws Exception {
      final String key = "k-" + visibility;
      final String value = "k-" + visibility;
      final CountDownLatch after = new CountDownLatch(1);
      final CountDownLatch afterContinue = new CountDownLatch(1);
      final CountDownLatch before = new CountDownLatch(1);
      cache.addListener(new EntryListener(before, afterContinue, after));

      switch (visibility) {
         case SIZE:
            assertEquals(0, cache.size());
            break;
         case GET:
            assertNull(cache.get(key));
            break;
      }

      Future<Void> ignore = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.put(key, value);
            return null;
         }
      });

      boolean signalled = before.await(30, TimeUnit.SECONDS);
      assertTrue("Timed out while waiting for before listener notification",
            signalled);

      switch (visibility) {
         case SIZE:
            assertEquals(0, cache.size());
            break;
         case GET:
            assertNull(cache.get(key));
            break;
      }

      // Let the isPre=false callback continue
      afterContinue.countDown();

      signalled = after.await(30, TimeUnit.SECONDS);
      assertTrue("Timed out while waiting for after listener notification",
            signalled);

      switch (visibility) {
         case SIZE:
            assertEquals(1, cache.size());
            break;
         case GET:
            Object retVal = cache.get(key);
            assertNotNull(retVal);
            assertEquals(retVal, value);
            break;
      }
      ignore.get(5, TimeUnit.SECONDS);
   }

   @Listener
   public static class EntriesEvictedListener {
      // Use a different listener class for @CacheEntriesEvicted vs
      // @CacheEntryEvicted to tests both callbacks separately

      Log log = LogFactory.getLog(EntriesEvictedListener.class);

      final CountDownLatch after;

      public EntriesEvictedListener(CountDownLatch after) {
         this.after = after;
      }

      @CacheEntriesEvicted
      @SuppressWarnings("unused")
      public void entryEvicted(Event e) {
         log.info("Cache entries evicted, now check in different thread");
         after.countDown();
         // Force a bit of delay in the listener so that lack of visibility
         // of changes in container can be appreciated more easily
         TestingUtil.sleepThread(1000);
      }
   }

   @Listener
   public static class CacheClearListener {

      Log log = LogFactory.getLog(CacheClearListener.class);

      final CyclicBarrier after;
      final CountDownLatch before;
      final CountDownLatch afterContinue;

      public CacheClearListener(CountDownLatch before,
            CountDownLatch afterContinue, CyclicBarrier after) {
         this.before = before;
         this.after = after;
         this.afterContinue = afterContinue;
      }

      @CacheEntryRemoved
      @SuppressWarnings("unused")
      public void entryTouched(Event e) {
         if (!e.isPre()) {
            log.infof("Cache entry removed, event is: %s", e);
            try {
               after.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e1) {
               Thread.currentThread().interrupt();
            } catch (BrokenBarrierException e1) {
               throw new IllegalStateException(e1);
            } catch (TimeoutException e1) {
               throw new IllegalStateException(e1);
            }
            // Force a bit of delay in the listener so that lack of visibility
            // of changes in container can be appreciated more easily
            TestingUtil.sleepThread(1000);
         } else {
            before.countDown();
            try {
               boolean signalled = afterContinue.await(30, TimeUnit.SECONDS);
               assertTrue("Timed out while waiting for post listener event to execute",
                     signalled);
            } catch (InterruptedException e1) {
               Thread.currentThread().interrupt();
            }
         }
      }

   }


   @Listener
   public static class EntryListener {

      Log log = LogFactory.getLog(EntryListener.class);

      final CountDownLatch after;
      final CountDownLatch before;
      final CountDownLatch afterContinue;

      public EntryListener(CountDownLatch before,
            CountDownLatch afterContinue, CountDownLatch after) {
         this.before = before;
         this.after = after;
         this.afterContinue = afterContinue;
      }

      @CacheEntryEvicted
      @SuppressWarnings("unused")
      public void entryEvicted(Event e) {
         log.info("Cache entry evicted, now check in different thread");
         after.countDown();
         // Force a bit of delay in the listener so that lack of visibility
         // of changes in container can be appreciated more easily
         TestingUtil.sleepThread(1000);
      }

      @CacheEntryCreated
      @CacheEntryRemoved
      @SuppressWarnings("unused")
      public void entryTouched(Event e) {
         if (!e.isPre()) {
            log.info("Cache entry touched, now check in different thread");
            after.countDown();
            // Force a bit of delay in the listener so that lack of visibility
            // of changes in container can be appreciated more easily
            TestingUtil.sleepThread(1000);
         } else {
            before.countDown();
            try {
               boolean signalled = afterContinue.await(30, TimeUnit.SECONDS);
               assertTrue("Timed out while waiting for post listener event to execute",
                     signalled);
            } catch (InterruptedException e1) {
               Thread.currentThread().interrupt();
            }
         }
      }

   }

   public static abstract class WithAssertListener {

      Log log = LogFactory.getLog(WithAssertListener.class);

      final CountDownLatch latch;
      volatile boolean assertNotNull;
      volatile boolean assertValue;

      protected WithAssertListener(CountDownLatch latch) {
         this.latch = latch;
      }

      protected void assertCacheContents(CacheEntryEvent e) {
         if (!e.isPre()) {
            log.info("Cache entry created, now check cache contents");
            Object value = e.getCache().get("k");
            if (value == null) {
               assertNotNull = false;
               assertValue = false;
            } else {
               assertNotNull = true;
               assertValue = value.equals("v");
            }
            // Force a bit of delay in the listener
            latch.countDown();
         }
      }

   }

   @Listener
   public static class EntryCreatedWithAssertListener extends WithAssertListener {

      protected EntryCreatedWithAssertListener(CountDownLatch latch) {
         super(latch);
      }

      @CacheEntryCreated
      @SuppressWarnings("unused")
      public void entryCreated(CacheEntryEvent e) {
         assertCacheContents(e);
      }

   }

   @Listener
   public static class EntryModifiedWithAssertListener extends WithAssertListener {

      protected EntryModifiedWithAssertListener(CountDownLatch latch) {
         super(latch);
      }

      @CacheEntryCreated
      @CacheEntryModified
      @SuppressWarnings("unused")
      public void entryCreated(CacheEntryEvent e) {
         assertCacheContents(e);
      }

   }

   private enum Visibility {
      SIZE, GET
   }

}

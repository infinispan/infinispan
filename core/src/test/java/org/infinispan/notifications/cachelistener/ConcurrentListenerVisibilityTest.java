/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.notifications.cachelistener;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests visibility of effects of cache operations on a separate thread once
 * a cache listener event has been consumed for the corresponding cache
 * operation.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "notifications.cachelistener.ConcurrentListenerVisibilityTest",
      enabled = false, description = "Disabled cos none of the tests pass :(")
@CleanupAfterMethod
public class ConcurrentListenerVisibilityTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createLocalCacheManager(false);
   }

   public void testSizeVisibility() throws Exception {
      updateCache(Visibility.SIZE);
   }

   public void testGetVisibility() throws Exception {
      updateCache(Visibility.GET);
   }

   public void testGetVisibilityWithinEntryCreatedListener() throws Exception {
      updateCacheAssertInListener(new EntryCreatedWithAssertListener(new CountDownLatch(1)));
   }

   public void testGetVisibilityWithinEntryModifiedListener() throws Exception {
      updateCacheAssertInListener(new EntryModifiedWithAssertListener(new CountDownLatch(1)));
   }

   private void updateCacheAssertInListener(WithAssertListener listener) throws Exception {
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
      final CountDownLatch entryCreatedLatch = new CountDownLatch(1);
      cache.addListener(new EntryCreatedListener(entryCreatedLatch));

      switch (visibility) {
         case SIZE:
            assert cache.size() == 0;
            break;
         case GET:
            assert cache.get(key) == null;
            break;
      }

      Future<Void> ignore = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.put(key, value);
            return null;
         }
      });

      entryCreatedLatch.await(30, TimeUnit.SECONDS);

      switch (visibility) {
         case SIZE:
            int size = cache.size();
            assert size == 1 : "size is: " + size;
            break;
         case GET:
            Object retVal = cache.get(key);
            assert retVal != null;
            assert retVal.equals(value): "retVal is: " + retVal;
            break;
      }
      ignore.get(5, TimeUnit.SECONDS);
   }

   @Listener
   public static class EntryCreatedListener {

      Log log = LogFactory.getLog(EntryCreatedListener.class);

      final CountDownLatch latch;

      public EntryCreatedListener(CountDownLatch latch) {
         this.latch = latch;
      }

      @CacheEntryCreated
      @SuppressWarnings("unused")
      public void entryCreated(CacheEntryCreatedEvent e) {
         if (!e.isPre()) {
            log.info("Cache entry created, now check in different thread");
            latch.countDown();
            // Force a bit of delay in the listener
            TestingUtil.sleepThread(3000);
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
               assertValue = value.equals("k");
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

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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
      cm = TestCacheManagerFactory.createLocalCacheManager(false);
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

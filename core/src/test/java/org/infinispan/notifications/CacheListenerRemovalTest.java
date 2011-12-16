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

/**
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "notifications.CacheListenerRemovalTest")
public class CacheListenerRemovalTest extends AbstractInfinispanTest {
   Cache<String, String> cache;
   CacheContainer cm;

   @BeforeMethod
   public void setUp() {
      cm = TestCacheManagerFactory.createLocalCacheManager(false);
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
      assert 0 == cache.getListeners().size();
      CacheListener l = new CacheListener(i);
      cache.addListener(l);
      assert 1 == cache.getListeners().size();
      assert cache.getListeners().contains(l);
      assert 0 == i.get();
      cache.get("x");
      assert 1 == i.get();

      // remove the replListener
      cache.removeListener(l);
      assert 0 == cache.getListeners().size();
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

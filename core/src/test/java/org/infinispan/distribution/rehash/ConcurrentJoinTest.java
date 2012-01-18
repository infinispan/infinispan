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
package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Test(groups = "functional", testName = "distribution.rehash.ConcurrentJoinTest", description = "See ISPN-1123")
public class ConcurrentJoinTest extends RehashTestBase {

   List<EmbeddedCacheManager> joinerManagers;
   List<Cache<Object, String>> joiners;

   static final int NUM_JOINERS = 4;

   void performRehashEvent(boolean offline) {
      joinerManagers = new CopyOnWriteArrayList<EmbeddedCacheManager>();
      joiners = new CopyOnWriteArrayList<Cache<Object, String>>(new Cache[NUM_JOINERS]);

      for (int i = 0; i < NUM_JOINERS; i++) {
         EmbeddedCacheManager joinerManager = addClusterEnabledCacheManager(new TransportFlags().withFD(true));
         joinerManager.defineConfiguration(cacheName, configuration);
         joinerManagers.add(joinerManager);
         joiners.set(i, null);
      }

      Thread[] threads = new Thread[NUM_JOINERS];
      for (int i = 0; i < NUM_JOINERS; i++) {
         final int ii = i;
         threads[i] = new Thread(new Runnable() {
            public void run() {
               EmbeddedCacheManager joinerManager = joinerManagers.get(ii);
               Cache<Object, String> joiner = joinerManager.getCache(cacheName);
               joiners.set(ii, joiner);
            }
         }, "ConcurrentJoinTest-Worker-" + i);
      }

      for (int i = 0; i < NUM_JOINERS; i++) {
         threads[i].start();
      }
      for (int i = 0; i < NUM_JOINERS; i++) {
         try {
            threads[i].join();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
   }

   @SuppressWarnings("unchecked")
   void waitForRehashCompletion() {
      List<CacheContainer> allCacheManagers = new ArrayList<CacheContainer>(cacheManagers);
      // Collection already contains all cache managers, no need to add more
      TestingUtil.blockUntilViewsReceived(60000, false, allCacheManagers);
      waitForClusterToForm(cacheName);
      int[] joinersPos = new int[NUM_JOINERS];
      for (int i = 0; i < NUM_JOINERS; i++) joinersPos[i] = locateJoiner(joinerManagers.get(i).getAddress());

      log.info("***>>> Joiners are in positions " + Arrays.toString(joinersPos));
      for (int i = 0; i < NUM_JOINERS; i++) {
         if (joinersPos[i] > caches.size())
            caches.add(joiners.get(i));
         else
            caches.add(joinersPos[i], joiners.get(i));
      }
   }
}

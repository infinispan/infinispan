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
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.concurrent.TimeUnit.SECONDS;

@Test(groups = "functional", testName = "distribution.rehash.ConcurrentJoinTest", enabled = false, description = "See ISPN-1123")
public class ConcurrentJoinTest extends RehashTestBase {

   List<EmbeddedCacheManager> joinerManagers;
   List<Cache<Object, String>> joiners;

   final int numJoiners = 4;

   void performRehashEvent(boolean offline) {
      joinerManagers = new CopyOnWriteArrayList<EmbeddedCacheManager>();
      joiners = new CopyOnWriteArrayList<Cache<Object, String>>(new Cache[numJoiners]);

      for (int i = 0; i < numJoiners; i++) {
         EmbeddedCacheManager joinerManager = addClusterEnabledCacheManager(true);
         joinerManager.defineConfiguration(cacheName, configuration);
         joinerManagers.add(joinerManager);
         joiners.set(i, null);
      }

      Thread[] threads = new Thread[numJoiners];
      for (int i = 0; i < numJoiners; i++) {
         final int ii = i;
         threads[i] = new Thread(new Runnable() {
            public void run() {
               EmbeddedCacheManager joinerManager = joinerManagers.get(ii);
               Cache<Object, String> joiner = joinerManager.getCache(cacheName);
               joiners.set(ii, joiner);
            }
         }, "ConcurrentJoinTest-Worker-" + i);
      }

      for (int i = 0; i < numJoiners; i++) {
         threads[i].start();
      }
      for (int i = 0; i < numJoiners; i++) {
         try {
            threads[i].join();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
   }

   @SuppressWarnings("unchecked")
   void waitForRehashCompletion() {
      List<Cache> allCaches = new ArrayList<Cache>(caches);
      allCaches.addAll(joiners);
      TestingUtil.blockUntilViewsReceived(60000, false, allCaches);
      waitForJoinTasksToComplete(SECONDS.toMillis(480), joiners.toArray(new Cache[numJoiners]));
      int[] joinersPos = new int[numJoiners];
      for (int i = 0; i < numJoiners; i++) joinersPos[i] = locateJoiner(joinerManagers.get(i).getAddress());

      log.info("***>>> Joiners are in positions " + Arrays.toString(joinersPos));
      for (int i = 0; i < numJoiners; i++) {
         if (joinersPos[i] > caches.size())
            caches.add(joiners.get(i));
         else
            caches.add(joinersPos[i], joiners.get(i));
      }
   }
}

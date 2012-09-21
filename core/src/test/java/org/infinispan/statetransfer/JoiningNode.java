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

package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Represents a joining node, designed for state transfer related tests.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class JoiningNode {

   private final EmbeddedCacheManager cm;
   private final CountDownLatch latch;
   private final MergeOrViewChangeListener listener;

   public JoiningNode(EmbeddedCacheManager cm) {
      this.cm = cm;
      latch = new CountDownLatch(1);
      listener = new MergeOrViewChangeListener(latch);
      cm.addListener(listener);
   }

   public Cache getCache() {
      return cm.getCache();
   }

   public Cache getCache(String cacheName) {
      return cm.getCache(cacheName);
   }

   public void waitForJoin(long timeout, Cache... caches) throws InterruptedException {
      // Wait for either a merge or view change to happen
      latch.await(timeout, TimeUnit.MILLISECONDS);
      // Wait for the state transfer to end
      TestingUtil.waitForRehashToComplete(caches);
   }

   private boolean isStateTransferred() {
      return !listener.merged;
   }

   void verifyStateTransfer(Callable<Void> verify) throws Exception {
      if (isStateTransferred())
         verify.call();
   }

}

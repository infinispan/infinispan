/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distexec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

/**
 * Tests org.infinispan.distexec.DistributedExecutorService in REPL_SYNC mode
 * 
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "distexec.ReplSyncDistributedExecutorTest")
public class ReplSyncDistributedExecutorTest extends DistributedExecutorTest {

   public static AtomicInteger ReplSyncDistributedExecutorTestCancelCounter = new AtomicInteger();

   public ReplSyncDistributedExecutorTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected String cacheName() {
      return "DistributedExecutorTest-REPL_SYNC";
   }

   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   /**
    * We use static counter in superclass and in parallel test suite we have to use separate counter
    * hence ReplSyncDistributedExecutorTestCancelCounter
    * 
    */
   public void testTaskCancellation() throws Exception {
      DistributedExecutorService des = createDES(getCache());
      List<Address> l = getCache().getAdvancedCache().getRpcManager().getTransport().getMembers();
      List<Address> members = new ArrayList<Address>(l);
      members.remove(getCache().getAdvancedCache().getRpcManager().getAddress());

      DistributedTaskBuilder<Integer> tb = des.createDistributedTaskBuilder(new MyLongRunningCallable());
      final Future<Integer> future = des.submit(members.get(0), tb.build());
      eventually(new Condition() {

         @Override
         public boolean isSatisfied() throws Exception {
            return ReplSyncDistributedExecutorTestCancelCounter.get() >= 1;
         }
      });
      future.cancel(true);
      boolean taskCancelled = false;
      Throwable root = null;
      try {
         future.get();
      } catch (Exception e) {
         root = e;
         while (root.getCause() != null) {
            root = root.getCause();
         }
         // task canceled with root exception being InterruptedException
         taskCancelled = root.getClass().equals(InterruptedException.class);
      }
      assert taskCancelled : "Dist task not cancelled " + root;
      assert future.isCancelled();
      assert future.isDone();
   }
   static class MyLongRunningCallable implements Callable<Integer>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -6110011263261397071L;

      @Override
      public Integer call() throws Exception {
         CountDownLatch latch = new CountDownLatch(1);
         ReplSyncDistributedExecutorTestCancelCounter.incrementAndGet();
         latch.await(5000, TimeUnit.MILLISECONDS);
         return 1;
      }
   }
}



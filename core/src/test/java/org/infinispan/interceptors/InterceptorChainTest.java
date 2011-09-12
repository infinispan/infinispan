/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.interceptors;

import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link InterceptorChain} logic
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "interceptors.InterceptorChainTest")
public class InterceptorChainTest {

   private static final Log log = LogFactory.getLog(InterceptorChainTest.class);

   public void testConcurrentAddRemove() throws Exception {
      CountDownLatch delayRemoveLatch = new CountDownLatch(1);
      InterceptorChain ic = new MockInterceptorChain(new CallInterceptor(), delayRemoveLatch);
      ic.addInterceptor(new CacheMgmtInterceptor(), 1);
      int nbWriters = 2;
      CyclicBarrier barrier = new CyclicBarrier(nbWriters + 1);
      List<Future<Void>> futures = new ArrayList<Future<Void>>(nbWriters);
      ExecutorService executorService = Executors.newCachedThreadPool();
      try {
         futures.add(executorService.submit(new ChainAdd(ic, barrier)));
         futures.add(executorService.submit(new ChainRemove(ic, barrier)));
         barrier.await(); // wait for all threads to be ready
         barrier.await(); // wait for all threads to finish
         log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
         for (Future<Void> future : futures) future.get();
      } finally {
         executorService.shutdownNow();
      }
   }

   class MockInterceptorChain extends InterceptorChain {
      final CountDownLatch delayRemoveLatch;

      public MockInterceptorChain(CommandInterceptor first, CountDownLatch delayRemoveLatch) {
         super(first);
         this.delayRemoveLatch = delayRemoveLatch;
      }

      @Override
      protected boolean isFirstInChain(Class<? extends CommandInterceptor> clazz) {
         try {
            delayRemoveLatch.await(5, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
         return super.isFirstInChain(clazz);
      }
   }

   abstract class InterceptorChainUpdater implements Callable<Void> {
      final InterceptorChain ic;
      final CyclicBarrier barrier;

      InterceptorChainUpdater(InterceptorChain ic, CyclicBarrier barrier) {
         this.ic = ic;
         this.barrier = barrier;
      }
   }

   class ChainAdd extends InterceptorChainUpdater {
      ChainAdd(InterceptorChain ic, CyclicBarrier barrier) {
         super(ic, barrier);
      }

      @Override
      public Void call() throws Exception {
         try {
            log.debug("Wait for all executions paths to be ready to perform calls");
            barrier.await();
            ic.addInterceptor(new CacheMgmtInterceptor(), 1);
            return null;
         } finally {
            log.debug("Wait for all execution paths to finish");
            barrier.await();
         }
      }
   }

   class ChainRemove extends InterceptorChainUpdater {
      ChainRemove(InterceptorChain ic, CyclicBarrier barrier) {
         super(ic, barrier);
      }

      @Override
      public Void call() throws Exception {
         try {
            log.debug("Wait for all executions paths to be ready to perform calls");
            barrier.await();
            // Thread.sleep(5000); // Wait long enough to allow
            ic.removeInterceptor(CacheMgmtInterceptor.class);
            return null;
         } finally {
            log.debug("Wait for all execution paths to finish");
            barrier.await();
         }
      }
   }

}

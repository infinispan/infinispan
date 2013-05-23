/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.replication;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.ReplicationQueueImpl;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Verifies that concurrent flushes are handled properly. These can occur when both flushes due to queue max size
 * being exceeded and interval based queue flushes occur at exactly the same time. The test verifies that order of
 * operations is guaranteed under these circumstances.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "replication.ConcurrentFlushReplQueueTest")
public class ConcurrentFlushReplQueueTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(CacheMode.REPL_ASYNC)
            .async().useReplQueue(true)
            .replQueueInterval(1000)
            .replQueueMaxElements(2)
            .replQueue(new MockReplQueue());
      CacheContainer first = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      CacheContainer second = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(first, second);
      // wait for the coordinator to install the balanced CH, otherwise StateTransferInterceptor will duplicate the command (via forwarding)
      waitForClusterToForm();
   }

   public void testConcurrentFlush(Method m) throws Exception {
      Cache cache1 = cache(0);
      Cache cache2 = cache(1);
      CountDownLatch intervalFlushLatch = new CountDownLatch(1);
      CountDownLatch secondPutLatch = new CountDownLatch(1);
      CountDownLatch removeCompletedLatch = new CountDownLatch(1);
      MockReplQueue.intervalFlushLatch = intervalFlushLatch;
      MockReplQueue.secondPutLatch = secondPutLatch;
      MockReplQueue.removeCompletedLatch = removeCompletedLatch; 
      final String k = "k-" + m.getName();
      final String v = "v-" + m.getName();
      cache1.put(k, v);
      // Wait for periodic repl queue task to try repl the single modification
      secondPutLatch.await(10, TimeUnit.SECONDS);
      // Put something random so that after remove call, the element number exceeds
      cache1.put("k-blah","v-blah");
      cache1.remove(k);
      // Wait for remove to go over draining the queue
      removeCompletedLatch.await(1000, TimeUnit.MILLISECONDS);
      // Once remove executed, now let the interval flush continue
      intervalFlushLatch.countDown();
      // Wait for periodic flush to send modifications over the wire
      TestingUtil.sleepThread(500);
      assert !cache2.containsKey(k);
   }

   public static class MockReplQueue extends ReplicationQueueImpl {
      static final Log log = LogFactory.getLog(MockReplQueue.class);
      static CountDownLatch intervalFlushLatch;
      static CountDownLatch secondPutLatch;
      static CountDownLatch removeCompletedLatch;

      @Override
      protected List<ReplicableCommand> drainReplQueue() {
         log.debugf("drainReplQueue called");
         List<ReplicableCommand> drained = super.drainReplQueue();
         try {
            if (drained.size() > 0 && Thread.currentThread().getName().startsWith("Scheduled-")) {
               log.debugf("Drained the put command on the replication thread: %s", drained);
               secondPutLatch.countDown();
                // Wait a max of 5 seconds, because if a remove could have gone through,
               // it would have done it in that time. If it hasn't and the test passes,
               // it means that correct synchronization is in place.
               intervalFlushLatch.await(5, TimeUnit.SECONDS);
            } else if (drained.size() > 0) {
               log.debugf("Drained the put+remove commands on the main thread: %s", drained);
               removeCompletedLatch.countDown();
            }
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
         return drained;
      }
   }
}

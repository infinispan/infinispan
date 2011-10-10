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
package org.infinispan.replication;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Tests ReplicationQueue's functionality.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "replication.ReplicationQueueTest")
public class ReplicationQueueTest extends MultipleCacheManagersTest {

   private static final int REPL_QUEUE_INTERVAL = 5000;
   private static final int REPL_QUEUE_MAX_ELEMENTS = 10;
   long creationTime;

   protected void createCacheManagers() throws Throwable {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setReplicationQueueScheduledExecutorFactoryClass(ReplQueueTestScheduledExecutorFactory.class.getName());
      globalConfiguration.setReplicationQueueScheduledExecutorProperties(ReplQueueTestScheduledExecutorFactory.myProps);
      CacheContainer first = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      CacheContainer second = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      registerCacheManager(first, second);

      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC, true);
      config.setUseReplQueue(true);
      config.setReplQueueInterval(REPL_QUEUE_INTERVAL);
      config.setReplQueueMaxElements(REPL_QUEUE_MAX_ELEMENTS);
      creationTime = System.currentTimeMillis();
      manager(0).defineConfiguration("replQueue", config);

      Configuration conf2 = config.clone();
      conf2.setUseReplQueue(false);
      manager(1).defineConfiguration("replQueue", conf2);
   }

   /**
    * tests that the replication queue will use an appropriate executor defined through
    * <tt>replicationQueueScheduledExecutor</tt> config param.
    */
   @Test(dependsOnMethods = "testReplicationBasedOnTime")
   public void testAppropriateExecutorIsUsed() {
      assert ReplQueueTestScheduledExecutorFactory.methodCalled;
      assert ReplQueueTestScheduledExecutorFactory.command != null;
      assert ReplQueueTestScheduledExecutorFactory.delay == REPL_QUEUE_INTERVAL;
      assert ReplQueueTestScheduledExecutorFactory.initialDelay == REPL_QUEUE_INTERVAL;
      assert ReplQueueTestScheduledExecutorFactory.unit == TimeUnit.MILLISECONDS;
   }

   /**
    * Make sure that replication will occur even if <tt>replQueueMaxElements</tt> are not reached, but the
    * <tt>replQueueInterval</tt> is reached.
    */
   public void testReplicationBasedOnTime() throws InterruptedException {
      
      Cache cache1 = cache(0, "replQueue");
      Cache cache2 = cache(1, "replQueue");
      
      //only place one element, queue size is 10. 
      cache1.put("key", "value");
      ReplicationQueue replicationQueue = TestingUtil.extractComponent(cache1, ReplicationQueue.class);
      assert replicationQueue != null;
      assert replicationQueue.getElementsCount() == 1;
      assert cache2.get("key") == null;
      assert cache1.get("key").equals("value");

      ReplQueueTestScheduledExecutorFactory.command.run();

      //in next 5 secs, expect the replication to occur
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 5000) {
         if (cache2.get("key") != null) break;
         Thread.sleep(50);
      }
      assert cache2.get("key").equals("value");
      assert replicationQueue.getElementsCount() == 0;
   }

   /**
    * Make sure that replication will occur even if <tt>replQueueMaxElements</tt> are not reached, but the
    * <tt>replQueueInterval</tt> is reached.
    */
   public void testReplicationBasedOnTimeWithTx() throws Exception {
      Cache cache1 = cache(0, "replQueue");
      Cache cache2 = cache(1, "replQueue");
      
      //only place one element, queue size is 10.
      TransactionManager transactionManager = TestingUtil.getTransactionManager(cache1);
      transactionManager.begin();
      cache1.put("key", "value");
      transactionManager.commit();

      ReplicationQueue replicationQueue = TestingUtil.extractComponent(cache1, ReplicationQueue.class);
      assert replicationQueue != null;
      assert replicationQueue.getElementsCount() == 1;
      assert cache2.get("key") == null;
      assert cache1.get("key").equals("value");

      ReplQueueTestScheduledExecutorFactory.command.run();

      //in next 5 secs, expect the replication to occur
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 5000) {
         if (cache2.get("key") != null) break;
         Thread.sleep(50);
      }
      assert cache2.get("key").equals("value");
      assert replicationQueue.getElementsCount() == 0;
   }


   /**
    * Make sure that replication will occur even if <tt>replQueueMaxElements</tt> is reached, but the
    * <tt>replQueueInterval</tt> is not reached.
    */
   public void testReplicationBasedOnSize() throws Exception {
      
      Cache cache1 = cache(0, "replQueue");
      Cache cache2 = cache(1, "replQueue");
      
      //place 10 elements, queue size is 10.
      for (int i = 0; i < REPL_QUEUE_MAX_ELEMENTS; i++) {
         cache1.put("key" + i, "value" + i);
      }
      //expect that in next 3 secs all commands are replicated
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 3000) {
         if (cache2.size() == REPL_QUEUE_MAX_ELEMENTS) break;
         Thread.sleep(50);
      }
      for (int i = 0; i < REPL_QUEUE_MAX_ELEMENTS; i++) {
         assert cache2.get("key" + i).equals("value" + i);
      }
   }

   /**
    * Make sure that replication will occur even if <tt>replQueueMaxElements</tt> is reached, but the
    * <tt>replQueueInterval</tt> is not reached.
    */
   public void testReplicationBasedOnSizeWithTx() throws Exception {
      
      Cache cache1 = cache(0, "replQueue");
      Cache cache2 = cache(1, "replQueue");
      
      //only place one element, queue size is 10.
      TransactionManager transactionManager = TestingUtil.getTransactionManager(cache1);
      for (int i = 0; i < REPL_QUEUE_MAX_ELEMENTS; i++) {
         transactionManager.begin();
         cache1.put("key" + i, "value" + i);
         transactionManager.commit();
      }
      //expect that in next 3 secs all commands are replicated
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 3000) {
         if (cache2.size() == REPL_QUEUE_MAX_ELEMENTS) break;
         Thread.sleep(50);
      }
      for (int i = 0; i < REPL_QUEUE_MAX_ELEMENTS; i++) {
         assert cache2.get("key" + i).equals("value" + i);
      }
   }

   /**
    * Test that replication queue works fine when multiple threads are putting into the queue.
    */
   public void testReplicationQueueMultipleThreads() throws Exception {
      final Cache cache1 = cache(0, "replQueue");
      Cache cache2 = cache(1, "replQueue");
      // put 10 elements in the queue from 5 different threads
      int numThreads = 5;
      final int numLoopsPerThread = 2;
      Thread[] threads = new Thread[numThreads];
      final CountDownLatch latch = new CountDownLatch(1);

      for (int i = 0; i < numThreads; i++) {
         final int i1 = i;
         threads[i] = new Thread() {
            int index = i1;

            public void run() {
               try {
                  latch.await();
               }
               catch (InterruptedException e) {
                  // do nothing
               }
               for (int j = 0; j < numLoopsPerThread; j++) {
                  cache1.put("key" + index + "_" + j, "value");
               }
            }
         };
         threads[i].start();
      }
      latch.countDown();
      // wait for threads to join
      for (Thread t : threads) t.join();

      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 3000) {
         if (cache2.size() == REPL_QUEUE_MAX_ELEMENTS) break;
         Thread.sleep(50);
      }
      assert cache2.size() == REPL_QUEUE_MAX_ELEMENTS;
      ReplicationQueue replicationQueue = TestingUtil.extractComponent(cache1, ReplicationQueue.class);
      assert replicationQueue.getElementsCount() == numThreads * numLoopsPerThread - REPL_QUEUE_MAX_ELEMENTS;
   }


   public static class ReplQueueTestScheduledExecutorFactory implements ScheduledExecutorFactory {
      static Properties myProps = new Properties();
      static boolean methodCalled = false;
      static Runnable command;
      static long initialDelay;
      static long delay;
      static TimeUnit unit;

      static {
         myProps.put("aaa", "bbb");
         myProps.put("ddd", "ccc");
      }

      public ScheduledExecutorService getScheduledExecutor(Properties p) {
         Properties toCompareWith = new Properties();
         for (Map.Entry<Object, Object> entry: myProps.entrySet())
            toCompareWith.setProperty((String) entry.getKey(), (String) entry.getValue()); 
         toCompareWith.setProperty("componentName", "replicationQueue-thread");
         toCompareWith.setProperty("threadPriority", "" + KnownComponentNames.getDefaultThreadPrio(KnownComponentNames.ASYNC_REPLICATION_QUEUE_EXECUTOR));
         assert p.equals(toCompareWith) : "Expected " + p + " but was " + toCompareWith;
         methodCalled = true;
         return new ScheduledThreadPoolExecutor(1) {
            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable commandP, long initialDelayP, long delayP, TimeUnit unitP) {
               command = commandP;
               initialDelay = initialDelayP;
               delay = delayP;
               unit = unitP;
               return null;
            }
         };
      }
   }


}

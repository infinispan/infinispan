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
package org.infinispan.lucene.testutils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

/**
 * CacheFactory useful to create clustered caches on-demand in several tests.
 * The same thread is used to actually create each cache, making it possible to create
 * several connected caches even though the testing suite in infinispan isolates different threads.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
@ThreadSafe
@SuppressWarnings("unchecked")
public class ClusteredCacheFactory {

   private final BlockingQueue<Configuration> requests = new SynchronousQueue<Configuration>();
   private final BlockingQueue<Cache> results = new SynchronousQueue<Cache>();
   private final ExecutorService executor = Executors.newSingleThreadExecutor();
   private final Configuration cfg;
   
   @GuardedBy("this") private boolean started = false;
   @GuardedBy("this") private boolean stopped = false;

   /**
    * Create a new ClusteredCacheFactory.
    * 
    * @param cfg defines the configuration used to build the caches
    */
   public ClusteredCacheFactory(Configuration cfg) {
      this.cfg = cfg;
   }

   /**
    * Create a cache using default configuration 
    * @throws InterruptedException if interrupted while waiting for the cache construction
    */
   public synchronized Cache createClusteredCache() throws InterruptedException {
      if (!started)
         throw new IllegalStateException("was not started");
      if (stopped)
         throw new IllegalStateException("was already stopped");
      requests.put(cfg);
      return results.take();
   }
   
   public Cache createClusteredCacheWaitingForNodesView(int expectedGroupSize) throws InterruptedException {
      Cache cache = createClusteredCache();
      TestingUtil.blockUntilViewReceived(cache, expectedGroupSize, 10000, false);
      return cache;
   }
   
   public synchronized void start() {
      if (started)
         throw new IllegalStateException("was already started");
      if (stopped)
         throw new IllegalStateException("was already stopped");
      started = true;
      executor.execute(new Worker());
   }

   public synchronized void stop() {
      if (stopped)
         throw new IllegalStateException("was already stopped");
      if (!started)
         throw new IllegalStateException("was not started");
      stopped = true;
      executor.shutdownNow();
   }

   private class Worker implements Runnable {

      @Override
      public void run() {
         while (true) {
            try {
               Configuration configuration = requests.take();
               CacheContainer cacheContainer = TestCacheManagerFactory.createClusteredCacheManager(configuration);
               Cache cache = cacheContainer.getCache();
               results.put(cache);
            } catch (InterruptedException e) {
               return;
            }
         }
      }

   }

}

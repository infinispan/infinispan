/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import org.infinispan.lucene.CacheKey;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

/**
 * CacheFactory. This is currently needed as a workaround for ISPN-261 : Cachemanager instantiated in different threads in same JVM don't interact
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
@ThreadSafe
public class ClusteredCacheFactory {

   private final BlockingQueue<Configuration> requests = new SynchronousQueue<Configuration>();
   private final BlockingQueue<Cache<CacheKey, Object>> results = new SynchronousQueue<Cache<CacheKey, Object>>();
   private final ExecutorService executor = Executors.newFixedThreadPool(1);
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
    * @return
    * @throws InterruptedException
    */
   public synchronized Cache<CacheKey, Object> createClusteredCache() throws InterruptedException {
      if (!started)
         throw new IllegalStateException("was not started");
      if (stopped)
         throw new IllegalStateException("was already stopped");
      requests.put(cfg);
      return results.take();
   }
   
   public Cache<CacheKey, Object> createClusteredCacheWaitingForNodesView(int expectedGroupSize) throws InterruptedException {
      Cache<CacheKey, Object> cache = createClusteredCache();
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
               CacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(configuration);
               Cache<CacheKey, Object> cache = cacheManager.getCache();
               results.put(cache);
            } catch (InterruptedException e) {
               return;
            }
         }
      }

   }

}

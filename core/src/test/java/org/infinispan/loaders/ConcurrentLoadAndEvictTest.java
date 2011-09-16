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
package org.infinispan.loaders;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.config.CloneableConfigurationComponent;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.CacheLoaderInterceptor;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.context.Flag.SKIP_CACHE_STORE;

/**
 * Tests a thread going past the cache loader interceptor and the interceptor deciding that loading is not necessary,
 * then another thread rushing ahead and evicting the entry from memory.
 *
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "loaders.ConcurrentLoadAndEvictTest")
public class ConcurrentLoadAndEvictTest extends SingleCacheManagerTest {
   SlowDownInterceptor sdi;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      sdi = new SlowDownInterceptor();
      // we need a loader and a custom interceptor to intercept get() calls
      // after the CLI, to slow it down so an evict goes through first
      Configuration config = new Configuration().fluent()
         .loaders()
            .addCacheLoader(new DummyInMemoryCacheStore.Cfg())
         .customInterceptors()
            .add(sdi).after(InvocationContextInterceptor.class)
         .transaction().transactionalCache(false)
         .build();

      return TestCacheManagerFactory.createCacheManager(config);
   }
 
   public void testEvictBeforeRead() throws CacheLoaderException, ExecutionException, InterruptedException {
      cache = cacheManager.getCache();
      cache.put("a", "b");
      assert cache.get("a").equals("b");
      CacheLoader cl = TestingUtil.getCacheLoader(cache);
      assert cl != null;
      InternalCacheEntry se = cl.load("a");
      assert se != null;
      assert se.getValue().equals("b");

      // clear the cache
      cache.getAdvancedCache().withFlags(SKIP_CACHE_STORE).clear();

      se = cl.load("a");
      assert se != null;
      assert se.getValue().equals("b");

      // now attempt a concurrent get and evict.
      ExecutorService e = Executors.newFixedThreadPool(1);
      sdi.enabled = true;

      log.info("test::doing the get");

      // call the get
      Future<String> future = e.submit(new Callable<String>() {
         public String call() throws Exception {
            return (String) cache.get("a");
         }
      });

      // now run the evict.
      log.info("test::before the evict");
      cache.evict("a");
      log.info("test::after the evict");

      // make sure the get call, which would have gone past the cache loader interceptor first, gets the correct value.
      assert future.get().equals("b");

      // disable the SlowDownInterceptor
      sdi.enabled = false;

      // and check that the key actually has been evicted
      assert !TestingUtil.extractComponent(cache, DataContainer.class).containsKey("a");

      e.shutdownNow();
   }

   public static class SlowDownInterceptor extends CommandInterceptor implements CloneableConfigurationComponent{
   
      private static final long serialVersionUID = 8790944676490291484L;
   
      volatile boolean enabled = false;
      transient CountDownLatch getLatch = new CountDownLatch(1);
      transient CountDownLatch evictLatch = new CountDownLatch(1);

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         if (enabled) {
            log.trace("Wait for evict to give go ahead...");
            if (!evictLatch.await(60000, TimeUnit.MILLISECONDS))
               throw new TimeoutException("Didn't see get after 60 seconds!");
         }
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            log.trace("After get, now let evict go through");
            if (enabled) getLatch.countDown();
         }
      }

      @Override
      public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
         if (enabled) {
            evictLatch.countDown();
            log.trace("Wait for get to finish...");
            if (!getLatch.await(60000, TimeUnit.MILLISECONDS))
               throw new TimeoutException("Didn't see evict after 60 seconds!");
         }
         return invokeNextInterceptor(ctx, command);
      }
      public SlowDownInterceptor clone(){
         try {
            return (SlowDownInterceptor) super.clone();
         } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Should not happen", e);
         }
      }    
   }
}

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

package org.infinispan.interceptors;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * Tests visibility of effects of cache operations on a separate thread once
 * they've passed a particular interceptor barrier related to the cache
 * operation.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "interceptors.ConcurrentInterceptorVisibilityTest")
public class ConcurrentInterceptorVisibilityTest extends AbstractInfinispanTest {

   public void testSizeVisibility() throws Exception {
      updateCache(Visibility.SIZE);
   }

   public void testGetVisibility() throws Exception {
      updateCache(Visibility.GET);
   }

   private void updateCache(final Visibility visibility) throws Exception {
      final String key = "k-" + visibility;
      final String value = "k-" + visibility;
      final CountDownLatch entryCreatedLatch = new CountDownLatch(1);
      final EntryCreatedInterceptor interceptor = new EntryCreatedInterceptor(entryCreatedLatch);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.customInterceptors().addInterceptor()
            .interceptor(interceptor)
            .before(EntryWrappingInterceptor.class);

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() throws Exception {
            final Cache<Object,Object> cache = cm.getCache();

            switch (visibility) {
               case SIZE:
                  assert cache.size() == 0;
                  break;
               case GET:
                  assert cache.get(key) == null;
                  break;
            }

            Future<Void> ignore = ConcurrentInterceptorVisibilityTest
                     .this.fork(new Callable<Void>() {
               @Override
               public Void call() throws Exception {
                  cache.put(key, value);
                  return null;
               }
            });

            entryCreatedLatch.await(30, TimeUnit.SECONDS);

            switch (visibility) {
               case SIZE:
                  int size = cache.size();
                  assert size == 1 : "size is: " + size;
                  assert interceptor.assertKeySet;
                  break;
               case GET:
                  Object retVal = cache.get(key);
                  assert retVal != null;
                  assert retVal.equals(value): "retVal is: " + retVal;
                  assert interceptor.assertKeySet;
                  break;
            }

            ignore.get(5, TimeUnit.SECONDS);
         }
      });
   }

   private enum Visibility {
      SIZE, GET
   }

   public static class EntryCreatedInterceptor extends BaseCustomInterceptor {

      Log log = LogFactory.getLog(EntryCreatedInterceptor.class);

      final CountDownLatch latch;
      volatile boolean assertKeySet;

      private EntryCreatedInterceptor(CountDownLatch latch) {
         this.latch = latch;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx,
               PutKeyValueCommand command) throws Throwable {
         // First execute the operation itself
         Object ret = super.visitPutKeyValueCommand(ctx, command);
         assertKeySet = (cache.keySet().size() == 1);
         // After entry has been committed to the container
         log.info("Cache entry created, now check in different thread");
         latch.countDown();
         // Force a bit of delay in the listener
         TestingUtil.sleepThread(3000);
         return ret;
      }

   }

}

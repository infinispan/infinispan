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
package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.AbstractInProcessFuture;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Tests concurrent startup of replicated and distributed caches
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "distribution.ConcurrentStartWithReplTest", groups = "functional")
public class ConcurrentStartWithReplTest extends AbstractInfinispanTest {

   private ConfigurationBuilder replCfg, distCfg;

   @BeforeTest
   public void setUp() {
      replCfg = MultipleCacheManagersTest.getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      replCfg.clustering().stateTransfer().fetchInMemoryState(true);

      distCfg = MultipleCacheManagersTest.getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      distCfg.clustering().stateTransfer().fetchInMemoryState(true);
   }

   @Test(timeOut = 60000)
   public void testSequence1() throws ExecutionException, InterruptedException {
      TestCacheManagerFactory.backgroundTestStarted(this);
      /*

      Sequence 1:

         C1 (repl) (becomes coord)
         C2 (dist)
         C1 (repl)
         C2 (dist)

         in the same thread.

       */

      doTest(true, false);

   }

   @Test(timeOut = 60000)
   public void testSequence2() throws ExecutionException, InterruptedException {
      TestCacheManagerFactory.backgroundTestStarted(this);
      /*

      Sequence 2:

         C1 (repl) (becomes coord)
         C2 (repl)
         C2 (dist)
         C1 (dist)

         in the same thread.

       */

      doTest(false, false);
   }

   @Test(timeOut = 60000)
   public void testSequence3() throws ExecutionException, InterruptedException {
      TestCacheManagerFactory.backgroundTestStarted(this);
      /*

      Sequence 3:

         C1 (repl) (becomes coord)
         C2 (repl)
         C1 (dist) (async thread)
         C2 (dist) (async thread)

         in the same thread, except the last two which are in separate threads

       */
      doTest(true, true);
   }

   @Test(timeOut = 60000)
   public void testSequence4() throws ExecutionException, InterruptedException {
      TestCacheManagerFactory.backgroundTestStarted(this);
      /*

      Sequence 4:

         C1 (repl) (becomes coord)
         C2 (repl)
         C2 (dist) (async thread)
         C1 (dist) (async thread)

         in the same thread, except the last two which are in separate threads

       */
      doTest(false, true);
   }

   private void doTest(boolean inOrder, boolean nonBlockingStartupForDist) throws ExecutionException, InterruptedException {
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(new ConfigurationBuilder());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(new ConfigurationBuilder());
      try {
         cm1.defineConfiguration("r", replCfg.build());
         cm1.defineConfiguration("d", distCfg.build());
         cm2.defineConfiguration("r", replCfg.build());
         cm2.defineConfiguration("d", distCfg.build());

         // first start the repl caches
         Cache<String, String> c1r = startCache(cm1, "r", false).get();
         c1r.put("key", "value");
         Cache<String, String> c2r = startCache(cm2, "r", false).get();
         TestingUtil.blockUntilViewsReceived(10000, c1r, c2r);
         TestingUtil.waitForRehashToComplete(c1r, c2r);
         assert "value".equals(c2r.get("key"));

         // now the dist ones
         Future<Cache<String, String>> c1df = startCache(inOrder ? cm1 : cm2, "d", nonBlockingStartupForDist);
         Future<Cache<String, String>> c2df = startCache(inOrder ? cm2 : cm1, "d", nonBlockingStartupForDist);
         Cache<String, String> c1d = c1df.get();
         Cache<String, String> c2d = c2df.get();

         c1d.put("key", "value");
         assert "value".equals(c2d.get("key"));
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   private Future<Cache<String, String>> startCache(final CacheContainer cm, final String cacheName, boolean nonBlockingStartup) {
      final Callable<Cache<String, String>> cacheCreator = new Callable<Cache<String, String>>() {

         @Override
         public Cache<String, String> call() throws Exception {
            Cache<String, String> c = cm.getCache(cacheName);
            return c;
         }
      };
      if (nonBlockingStartup) {
         return fork(cacheCreator);
      } else {
         return new AbstractInProcessFuture<Cache<String, String>>() {
            @Override
            public Cache<String, String> get() throws InterruptedException, ExecutionException {
               try {
                  return cacheCreator.call();
               } catch (Exception e) {
                  throw new ExecutionException(e);
               }
            }
         };
      }
   }

}



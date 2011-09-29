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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests basic org.infinispan.distexec.DistributedExecutorService functionality
 * 
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "distexec.BasicDistributedExecutorTest")
public class BasicDistributedExecutorTest extends AbstractCacheTest {

   public BasicDistributedExecutorTest() {
   }
   
   @Test(expectedExceptions = { IllegalStateException.class })
   public void testImproperCacheStateForDistribtuedExecutor() {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         Cache<Object, Object> cache = cacheManager.getCache();
         DistributedExecutorService des = new DefaultExecutorService(cache);
      } finally {
         cacheManager.stop();
      }
   }
   
   /**
    * Tests that we can invoke DistributedExecutorService on an Infinispan cluster having a single node
    * 
    * @throws Exception
    */
   public void testSingleCacheExecution() throws Exception {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager();
      try {
         Cache<Object, Object> cache = cacheManager.getCache();
         DistributedExecutorService des = new DefaultExecutorService(cache);
         Future<Integer> future = des.submit(new SimpleCallable());
         Integer r = future.get();
         assert r == 1;

         List<Future<Integer>> list = des.submitEverywhere(new SimpleCallable());
         assert list.size() == 1;
         for (Future<Integer> f : list) {
            assert f.get() == 1;
         }
      } finally {
         cacheManager.stop();
      }
   }


   static class SimpleCallable implements Callable<Integer>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = -8589149500259272402L;

      @Override
      public Integer call() throws Exception {
         return 1;
      }
   }
}

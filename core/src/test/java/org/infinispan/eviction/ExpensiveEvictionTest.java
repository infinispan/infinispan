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
package org.infinispan.eviction;

import java.util.concurrent.TimeUnit;

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "profiling", enabled = false, testName = "profiling.ExpensiveEvictionTest")
public class ExpensiveEvictionTest extends SingleCacheManagerTest {

   private final Integer MAX_CACHE_ELEMENTS = 10 * 1000 * 1000;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration cfg = new Configuration().fluent()
         .eviction().strategy(EvictionStrategy.LRU).maxEntries(MAX_CACHE_ELEMENTS)
         .expiration().wakeUpInterval(3000L)
         .build();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      return cm;
   }

   public void testSimpleEvictionMaxEntries() throws Exception {
      System.out.println("Max entries: " + MAX_CACHE_ELEMENTS);
      for (int i = 0; i < MAX_CACHE_ELEMENTS; i++) {
         Integer integer = Integer.valueOf(i);
         cache.put(integer, integer, 6, TimeUnit.HOURS);
         if (i % 50000 == 0) {
            System.out.println("Elemenents in cache: " + cache.size());
         }
      }
      System.out.println("Finished filling in cache. Now idle while eviting thread works....");
      Thread.sleep(TimeUnit.MILLISECONDS.convert(2, TimeUnit.HOURS));
   }

}

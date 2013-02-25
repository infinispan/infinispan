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
package org.infinispan.lucene.readlocks;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verifies a DistributedSegmentReadLocker can be built only on certain types of caches,
 * for example it shouldn't be allowed to use eviction: see ISPN-680
 * 
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "lucene.readlocks.ConfigurationCheckTest")
public class ConfigurationCheckTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configurationBuilder = CacheTestSupport.createLocalCacheConfiguration();
      configurationBuilder
         .eviction()
            .strategy(EvictionStrategy.LRU)
            .maxEntries(10)
            ;
      return TestCacheManagerFactory.createCacheManager(configurationBuilder);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testEvictionIsNotAllowed() {
      Cache<?, ?> c = cacheManager.getCache();
      new DistributedSegmentReadLocker((Cache<Object, Integer>) c, c, c, "lucene.readlocks.ConfigurationCheckTest");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testLocksCacheNullIsNotAllowed() {
      Cache<?, ?> c = cacheManager.getCache();
      new DistributedSegmentReadLocker(null, c, c, "lucene.readlocks.ConfigurationCheckTest");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testChunkCacheNullIsNotAllowed() {
      Cache<?, ?> c = cacheManager.getCache();
      new DistributedSegmentReadLocker((Cache<Object, Integer>) c, null, c, "lucene.readlocks.ConfigurationCheckTest");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testMetaDataNullIsNotAllowed() {
      Cache<?, ?> c = cacheManager.getCache();
      new DistributedSegmentReadLocker((Cache<Object, Integer>) c, c, null, "lucene.readlocks.ConfigurationCheckTest");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIndexNameNullIsNotAllowed() {
      Cache<?, ?> c = cacheManager.getCache();
      new DistributedSegmentReadLocker((Cache<Object, Integer>) c, c, c, null);
   }

}

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

import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
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
      Configuration configuration = new Configuration();
      configuration.setEvictionStrategy(EvictionStrategy.LRU);
      configuration.setEvictionMaxEntries(10);
      return TestCacheManagerFactory.createCacheManager(configuration);
   }
   
   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testEvictionIsNotAllowed() {
      cache = cacheManager.getCache();
      new DistributedSegmentReadLocker(cache, cache, cache, "lucene.readlocks.ConfigurationCheckTest");
   }

}

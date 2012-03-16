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

package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test that verifies the behaivour of Cache and CacheContainer.getCache() calls after
 * Cache and CacheContainer instances have been stopped. This emulates redeployment
 * scenarios under a situations where the CacheContainer is a shared resource.
 *
 * @author Galder Zamarre�o
 * @since 4.2
 */
@Test(groups = "functional", testName = "api.TerminatedCacheTest")
public class TerminatedCacheTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createLocalCacheManager(false);
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testCacheStopFollowedByGetCache() {
      Cache cache = cacheManager.getCache();
      cache.put("k", "v");
      cache.stop();
      Cache cache2 = cacheManager.getCache();
      cache2.put("k", "v2");
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testCacheStopFollowedByCacheOp() {
      Cache cache = cacheManager.getCache("big");
      cache.put("k", "v");
      cache.stop();
      cache.put("k", "v2");
   }

}

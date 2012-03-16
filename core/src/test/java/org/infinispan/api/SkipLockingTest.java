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

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Tests {@link Flag#SKIP_LOCKING} logic
 *
 * @author Galder Zamarre�o
 * @since 4.1
 */
@Test(groups = "functional", testName = "api.SkipLockingTest")
public class SkipLockingTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createLocalCacheManager(false);
   }

   public void testSkipLockingAfterPutWithoutTm(Method m) {
      String name = m.getName();
      AdvancedCache advancedCache = cacheManager.getCache().getAdvancedCache();
      advancedCache.put("k-" + name, "v-" + name);
      advancedCache.withFlags(Flag.SKIP_LOCKING).put("k-" + name, "v2-" + name);
   }

   public void testSkipLockingAfterPutWithTm(Method m) {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createLocalCacheManager(true);
      try {
         AdvancedCache advancedCache = cacheManager.getCache().getAdvancedCache();
         String name = m.getName();
         advancedCache.put("k-" + name, "v-" + name);
         advancedCache.withFlags(Flag.SKIP_LOCKING).put("k-" + name, "v2-" + name);
      } finally {
         cacheManager.stop();
      }
   }

}

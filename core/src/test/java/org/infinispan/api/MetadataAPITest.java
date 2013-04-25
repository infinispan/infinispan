/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.EmbeddedMetadata;
import org.infinispan.Metadata;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "api.MetadataAPITest")
public class MetadataAPITest extends SingleCacheManagerTest {

   AdvancedCache<Integer, String> advCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      advCache = cm.<Integer, String>getCache().getAdvancedCache();
      return cm;
   }

   public void testPutWithVersion() {
      final Integer key = 1;
      NumericVersion version = new NumericVersion(1);
      advCache.put(key, "v1", withVersion(version));
      CacheEntry cacheEntry = advCache.getCacheEntry(key);
      assertEquals(InequalVersionComparisonResult.EQUAL,
            version.compareTo(cacheEntry.getVersion()));
   }

   public void testConditionalReplaceWithVersion() {
      final Integer key = 2;
      NumericVersion version = new NumericVersion(1);
      advCache.put(key, "v1", new EmbeddedMetadata.Builder().version(version).build());
      NumericVersion newVersion = new NumericVersion(2);
      advCache.replace(key, "v1", "v2", withVersion(newVersion));
      CacheEntry cacheEntry = advCache.getCacheEntry(key);
      assertEquals(InequalVersionComparisonResult.EQUAL,
            newVersion.compareTo(cacheEntry.getVersion()));
   }

   public void testPutIfAbsentWithVersion() {
      final Integer key = 3;
      NumericVersion version = new NumericVersion(1);
      assertEquals(null, advCache.putIfAbsent(key, "v1", withVersion(version)));
      CacheEntry cacheEntry = advCache.getCacheEntry(key);
      assertEquals(InequalVersionComparisonResult.EQUAL,
            version.compareTo(cacheEntry.getVersion()));
   }

   private Metadata withVersion(EntryVersion version) {
      return new EmbeddedMetadata.Builder().version(version).build();
   }

}

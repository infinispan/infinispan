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
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.*;
import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;

/**
 * Tests cache API methods that take {@link Metadata} as parameter.
 *
 * @author Galder Zamarreño
 * @since 5.3
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
      assertEquals(EQUAL, version.compareTo(cacheEntry.getMetadata().version()));
   }

   public void testConditionalReplaceWithVersion() {
      final Integer key = 2;
      NumericVersion version = new NumericVersion(1);
      advCache.put(key, "v1", new EmbeddedMetadata.Builder().version(version).build());
      NumericVersion newVersion = new NumericVersion(2);
      advCache.replace(key, "v1", "v2", withVersion(newVersion));
      CacheEntry cacheEntry = advCache.getCacheEntry(key);
      assertEquals(EQUAL, newVersion.compareTo(cacheEntry.getMetadata().version()));
   }

   public void testPutIfAbsentWithVersion() {
      final Integer key = 3;
      NumericVersion version = new NumericVersion(1);
      assertEquals(null, advCache.putIfAbsent(key, "v1", withVersion(version)));
      CacheEntry cacheEntry = advCache.getCacheEntry(key);
      assertEquals(EQUAL, version.compareTo(cacheEntry.getMetadata().version()));
   }

   public void testPutAsyncWithVersion() throws Exception {
      final Integer key = 4;
      NumericVersion version = new NumericVersion(1);
      Future<String> f = advCache.putAsync(key, "v1", withVersion(version));
      assertNotNull(f);
      assertFalse(f.isCancelled());
      assertNull(f.get());
      assertTrue(f.isDone());
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals("v1", entry.getValue());
      assertEquals(EQUAL, version.compareTo(entry.getMetadata().version()));
   }

   public void testGetCustomMetadataForMortalEntries() throws Exception {
      final Integer key = 5;
      Metadata meta = new CustomMetadata(3000, -1);
      advCache.put(key, "v1", meta);
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals(meta, entry.getMetadata());
   }

   public void testGetCustomMetadataForTransientEntries() throws Exception {
      final Integer key = 6;
      Metadata meta = new CustomMetadata(-1, 3000);
      advCache.put(key, "v1", meta);
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals(meta, entry.getMetadata());
   }

   public void testGetCustomMetadataForTransientMortalEntries() throws Exception {
      final Integer key = 6;
      Metadata meta = new CustomMetadata(3000, 3000);
      advCache.put(key, "v1", meta);
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals(meta, entry.getMetadata());
   }

   public void testReplaceWithVersion() {
      final Integer key = 7;
      NumericVersion version = new NumericVersion(1);
      advCache.put(key, "v1", new EmbeddedMetadata.Builder().version(version).build());
      NumericVersion newVersion = new NumericVersion(2);
      advCache.replace(key, "v2", withVersion(newVersion));
      CacheEntry cacheEntry = advCache.getCacheEntry(key);
      assertEquals(EQUAL, newVersion.compareTo(cacheEntry.getMetadata().version()));
   }

   public void testOverrideImmortalCustomMetadata() {
      final Integer key = 8;
      Metadata meta = new CustomMetadata(-1, -1);
      advCache.put(key, "v1", meta);
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals(meta, entry.getMetadata());
      Metadata newMeta = new CustomMetadata(120000, 60000);
      advCache.put(key, "v2", newMeta);
      assertEquals(newMeta, advCache.getCacheEntry(key).getMetadata());
   }

   public void testOverrideMortalCustomMetadata() {
      final Integer key = 9;
      Metadata meta = new CustomMetadata(120000, -1);
      advCache.put(key, "v1", meta);
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals(meta, entry.getMetadata());
      Metadata newMeta = new CustomMetadata(240000, -1);
      advCache.put(key, "v2", newMeta);
      assertEquals(newMeta, advCache.getCacheEntry(key).getMetadata());
   }

   public void testOverrideTransientCustomMetadata() {
      final Integer key = 10;
      Metadata meta = new CustomMetadata(-1, 120000);
      advCache.put(key, "v1", meta);
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals(meta, entry.getMetadata());
      Metadata newMeta = new CustomMetadata(-1, 240000);
      advCache.put(key, "v2", newMeta);
      assertEquals(newMeta, advCache.getCacheEntry(key).getMetadata());
   }

   public void testOverrideTransientMortalCustomMetadata() {
      final Integer key = 10;
      Metadata meta = new CustomMetadata(60000, 120000);
      advCache.put(key, "v1", meta);
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals(meta, entry.getMetadata());
      Metadata newMeta = new CustomMetadata(120000, 240000);
      advCache.put(key, "v2", newMeta);
      assertEquals(newMeta, advCache.getCacheEntry(key).getMetadata());
   }

   private Metadata withVersion(EntryVersion version) {
      return new EmbeddedMetadata.Builder().version(version).build();
   }

   private class CustomMetadata implements Metadata, Metadata.Builder {

      private final long lifespan;
      private final long maxIdle;

      private CustomMetadata(long lifespan, long maxIdle) {
         this.lifespan = lifespan;
         this.maxIdle = maxIdle;
      }

      private CustomMetadata(Metadata template) {
         this.lifespan = template.lifespan();
         this.maxIdle = template.maxIdle();
      }

      @Override
      public long lifespan() {
         return lifespan;
      }

      @Override
      public long maxIdle() {
         return maxIdle;
      }

      @Override
      public EntryVersion version() {
         return null; // ignore
      }

      @Override
      public Builder builder() {
         return this; // ignore
      }

      @Override
      public Builder lifespan(long time, TimeUnit unit) {
         return new CustomMetadata(unit.toMillis(time), maxIdle);
      }

      @Override
      public Builder lifespan(long time) {
         return lifespan(time, TimeUnit.MILLISECONDS);
      }

      @Override
      public Builder maxIdle(long time, TimeUnit unit) {
         return new CustomMetadata(lifespan, unit.toMillis(time));
      }

      @Override
      public Builder maxIdle(long time) {
         return maxIdle(time, TimeUnit.MILLISECONDS);
      }

      @Override
      public Builder version(EntryVersion version) {
         return this;
      }

      @Override
      public Metadata build() {
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CustomMetadata that = (CustomMetadata) o;

         if (lifespan != that.lifespan) return false;
         if (maxIdle != that.maxIdle) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = (int) (lifespan ^ (lifespan >>> 32));
         result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
         return result;
      }

      @Override
      public String toString() {
         return "CustomMetadata{" +
               "lifespan=" + lifespan +
               ", maxIdle=" + maxIdle +
               '}';
      }
   }

}

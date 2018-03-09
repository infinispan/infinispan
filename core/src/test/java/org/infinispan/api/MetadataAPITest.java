package org.infinispan.api;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.EQUAL;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.functional.MetaParam;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

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
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
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
      advCache.put(key, "v1", withVersion(version));
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

   public void testPutWithLifespan() {
      final Integer key = 1;
      int lifespan = 1_000_000;
      advCache.put(key, "v1", withLifespan(lifespan));
      CacheEntry cacheEntry = advCache.getCacheEntry(key);
      assertEquals(lifespan, cacheEntry.getMetadata().lifespan());
   }

   public void testConditionalReplaceWithLifespan() {
      final Integer key = 2;
      long lifespan = 1_000_000;
      advCache.put(key, "v1", withLifespan(lifespan));
      long newLifespan = 2_000_000;
      advCache.replace(key, "v1", "v2", withLifespan(newLifespan));
      CacheEntry cacheEntry = advCache.getCacheEntry(key);
      assertEquals(newLifespan, cacheEntry.getMetadata().lifespan());
   }

   public void testPutIfAbsentWithLifespan() {
      final Integer key = 3;
      long lifespan = 1_000_000;
      assertEquals(null, advCache.putIfAbsent(key, "v1", withLifespan(lifespan)));
      CacheEntry cacheEntry = advCache.getCacheEntry(key);
      assertEquals(lifespan, cacheEntry.getMetadata().lifespan());
   }

   public void testPutAsyncWithLifespan() throws Exception {
      final Integer key = 4;
      long lifespan = 1_000_000;
      Future<String> f = advCache.putAsync(key, "v1", withLifespan(lifespan));
      assertNotNull(f);
      assertFalse(f.isCancelled());
      assertNull(f.get());
      assertTrue(f.isDone());
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals("v1", entry.getValue());
      assertEquals(lifespan, entry.getMetadata().lifespan());
   }

   public void testPutFunctionalWithLifespan() throws Exception {
      final Integer key = 4;
      long lifespan = 1_000_000;
      CompletableFuture<Void> f = WriteOnlyMapImpl.create(FunctionalMapImpl.create(advCache))
            .eval(key, view -> view.set("v1", new MetaParam.MetaLifespan(lifespan)));
      assertNotNull(f);
      assertFalse(f.isCancelled());
      assertNull(f.get());
      assertTrue(f.isDone());
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals("v1", entry.getValue());
      assertEquals(lifespan, entry.getMetadata().lifespan());
   }

   public void testReplaceFunctionalWithLifespan() throws Exception {
      final Integer key = 4;
      long lifespan = 1_000_000;
      CompletableFuture<Void> f = WriteOnlyMapImpl.create(FunctionalMapImpl.create(advCache))
            .eval(key, view -> view.set("v1", new MetaParam.MetaLifespan(lifespan)));
      assertNotNull(f);
      assertFalse(f.isCancelled());
      assertNull(f.get());
      assertTrue(f.isDone());
      long newLifespan = 2_000_000;
      f = WriteOnlyMapImpl.create(FunctionalMapImpl.create(advCache))
            .eval(key, view -> view.set("v2", new MetaParam.MetaLifespan(newLifespan)));
      assertNotNull(f);
      assertFalse(f.isCancelled());
      assertNull(f.get());
      assertTrue(f.isDone());
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals("v2", entry.getValue());
      assertEquals(newLifespan, entry.getMetadata().lifespan());
   }

   /**
    * See ISPN-6773.
    */
   public void testReplaceEmbeddedFunctionalWithLifespan() throws Exception {
      final Integer key = 4;
      long lifespan = 1_000_000;
      advCache.put(key, "v1", withLifespan(lifespan));
      long newLifespan = 2_000_000;
      CompletableFuture<Void> f = WriteOnlyMapImpl.create(FunctionalMapImpl.create(advCache))
            .eval(key, view -> view.set("v2", new MetaParam.MetaLifespan(newLifespan)));
      assertNotNull(f);
      assertFalse(f.isCancelled());
      assertNull(f.get());
      assertTrue(f.isDone());
      CacheEntry entry = advCache.getCacheEntry(key);
      assertEquals("v2", entry.getValue());
      assertEquals(newLifespan, entry.getMetadata().lifespan());
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
      advCache.put(key, "v1", withVersion(version));
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

   public void testPutForExternalReadWithVersion() {
      final Integer key = 11;
      NumericVersion version = new NumericVersion(1);
      advCache.putForExternalRead(key, "v1", withVersion(version));
      CacheEntry cacheEntry = advCache.getCacheEntry(key);
      assertEquals(version, cacheEntry.getMetadata().version());
   }

   public void testPutForExternalReadInDecaratedCacheWithVersion() {
      final Integer key = 12;
      NumericVersion version = new NumericVersion(1);
      // Flag forces decorated cache, but doesn't affect processing
      AdvancedCache<Integer, String> decoratedCache = advCache.withFlags(Flag.SKIP_STATISTICS);
      decoratedCache.putForExternalRead(key, "v1", withVersion(version));
      CacheEntry cacheEntry = decoratedCache.getCacheEntry(key);
      assertEquals(version, cacheEntry.getMetadata().version());
   }

   public void testPutForExternalReadWithLifespan() {
      final Integer key = 11;
      long lifespan = 1_000_000;
      advCache.putForExternalRead(key, "v1", withLifespan(lifespan));
      CacheEntry cacheEntry = advCache.getCacheEntry(key);
      assertEquals(lifespan, cacheEntry.getMetadata().lifespan());
   }

   public void testPutForExternalReadInDecaratedCacheWithLifespan() {
      final Integer key = 12;
      long lifespan = 1_000_000;
      // Flag forces decorated cache, but doesn't affect processing
      AdvancedCache<Integer, String> decoratedCache = advCache.withFlags(Flag.SKIP_STATISTICS);
      decoratedCache.putForExternalRead(key, "v1", withLifespan(lifespan));
      CacheEntry cacheEntry = decoratedCache.getCacheEntry(key);
      assertEquals(lifespan, cacheEntry.getMetadata().lifespan());
   }

   private Metadata withVersion(EntryVersion version) {
      return new EmbeddedMetadata.Builder().version(version).build();
   }

   private Metadata withLifespan(long lifespan) {
      return new EmbeddedMetadata.Builder().lifespan(lifespan).build();
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
      public InvocationRecord lastInvocation() {
         return null;
      }

      @Override
      public InvocationRecord invocation(CommandInvocationId id) {
         return null;
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
      public Builder invocation(CommandInvocationId id, Object previousValue, Metadata previousMetadata, long timestamp) {
         return this;
      }

      @Override
      public Builder invocations(InvocationRecord invocations) {
         return this;
      }

      @Override
      public InvocationRecord invocations() {
         return null;
      }

      @Override
      public Metadata build() {
         return this;
      }

      @Override
      public Builder merge(Metadata metadata) {
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

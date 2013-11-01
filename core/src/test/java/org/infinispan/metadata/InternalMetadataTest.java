package org.infinispan.metadata;

import org.infinispan.container.entries.AbstractInternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Tests the InternalMetadataImpl to check if it will not create a chain of InternalMetadataImpl
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "unit", testName = "metadata.InternalMetadataTest")
public class InternalMetadataTest {

   public void testWithMetadata() {
      TestMetadata metadata = new TestMetadata(1, 2);
      InternalMetadataImpl internalMetadata = new InternalMetadataImpl(metadata, 3, 4);
      assertInternalMetadataValues(internalMetadata, 1, 2, 3, 4);
      assertInternalMetadataActual(internalMetadata);

      InternalMetadataImpl internalMetadata2 = new InternalMetadataImpl(internalMetadata, 5, 6);
      assertInternalMetadataValues(internalMetadata2, 1, 2, 5, 6);
      assertInternalMetadataActual(internalMetadata2);
   }

   public void testWithInternalCacheEntry() {
      TestMetadata metadata = new TestMetadata(1, 2);
      InternalMetadataImpl internalMetadata = new InternalMetadataImpl(metadata, 3, 4);
      assertInternalMetadataValues(internalMetadata, 1, 2, 3, 4);
      assertInternalMetadataActual(internalMetadata);

      TestInternalCacheEntry cacheEntry = new TestInternalCacheEntry(internalMetadata, 5, 6);
      InternalMetadataImpl internalMetadata2 = new InternalMetadataImpl(cacheEntry);
      assertInternalMetadataValues(internalMetadata2, 1, 2, 5, 6);
      assertInternalMetadataActual(internalMetadata2);
   }

   public void testWithInternalCacheEntry2() {
      TestMetadata metadata = new TestMetadata(1, 2);
      TestInternalCacheEntry cacheEntry = new TestInternalCacheEntry(metadata, 3, 4);
      InternalMetadataImpl internalMetadata = new InternalMetadataImpl(cacheEntry);
      assertInternalMetadataValues(internalMetadata, 1, 2, 3, 4);
      assertInternalMetadataActual(internalMetadata);
   }

   private void assertInternalMetadataActual(InternalMetadataImpl metadata) {
      assertFalse("'actual' field must not be an InternalMetadataImpl", metadata.actual() instanceof InternalMetadataImpl);
   }

   private void assertInternalMetadataValues(InternalMetadata metadata, long lifespan, long maxIdle, long created,
                                             long lastUsed) {
      assertEquals("Wrong lifespan value.", lifespan, metadata.lifespan());
      assertEquals("Wrong maxIdle value.", maxIdle, metadata.maxIdle());
      assertEquals("Wrong created value.", created, metadata.created());
      assertEquals("Wrong lastUsed value.", lastUsed, metadata.lastUsed());
   }

   private class TestMetadata implements Metadata, Metadata.Builder {

      private final long lifespan;
      private final long maxIdle;

      private TestMetadata(long lifespan, long maxIdle) {
         this.lifespan = lifespan;
         this.maxIdle = maxIdle;
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
         return new TestMetadata(unit.toMillis(time), maxIdle);
      }

      @Override
      public Builder lifespan(long time) {
         return lifespan(time, TimeUnit.MILLISECONDS);
      }

      @Override
      public Builder maxIdle(long time, TimeUnit unit) {
         return new TestMetadata(lifespan, unit.toMillis(time));
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

         TestMetadata that = (TestMetadata) o;

         return lifespan == that.lifespan && maxIdle == that.maxIdle;

      }

      @Override
      public int hashCode() {
         int result = (int) (lifespan ^ (lifespan >>> 32));
         result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
         return result;
      }

      @Override
      public String toString() {
         return "TestMetadata{" +
               "lifespan=" + lifespan +
               ", maxIdle=" + maxIdle +
               '}';
      }
   }

   //Dummy class
   private class TestInternalCacheEntry extends AbstractInternalCacheEntry {

      private final Metadata metadata;
      private final long created;
      private final long lastUsed;

      private TestInternalCacheEntry(Metadata metadata, long created, long lastUsed) {
         super(null);
         this.metadata = metadata;
         this.created = created;
         this.lastUsed = lastUsed;
      }

      @Override
      public boolean isExpired(long now) {
         return false;
      }

      @Override
      public boolean isExpired() {
         return false;
      }

      @Override
      public boolean canExpire() {
         return false;
      }

      @Override
      public long getCreated() {
         return created;
      }

      @Override
      public long getLastUsed() {
         return lastUsed;
      }

      @Override
      public long getExpiryTime() {
         return 0;
      }

      @Override
      public void touch() {
      }

      @Override
      public void touch(long currentTimeMillis) {
      }

      @Override
      public void reincarnate() {
      }

      @Override
      public void reincarnate(long now) {
      }

      @Override
      public InternalCacheValue toInternalCacheValue() {
         return null;
      }

      @Override
      public Object getValue() {
         return null;
      }

      @Override
      public long getLifespan() {
         return metadata.lifespan();
      }

      @Override
      public long getMaxIdle() {
         return metadata.maxIdle();
      }

      @Override
      public Object setValue(Object value) {
         return null;
      }

      @Override
      public Metadata getMetadata() {
         return metadata;
      }
   }

}

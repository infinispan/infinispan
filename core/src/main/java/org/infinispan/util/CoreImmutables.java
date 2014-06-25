package org.infinispan.util;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commons.util.Immutables;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.metadata.Metadata;

/**
 * Factory for generating immutable type wrappers for core types.
 *
 * @author Jason T. Greene
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 * @since 4.0
 */
public class CoreImmutables extends Immutables {

   /**
    * Wraps a {@link InternalCacheEntry}} with an immutable {@link InternalCacheEntry}}. There is no copying involved.
    *
    * @param entry the internal cache entry to wrap.
    * @return an immutable {@link InternalCacheEntry}} wrapper that delegates to the original entry.
    */
   public static <K, V> InternalCacheEntry<K, V> immutableInternalCacheEntry(InternalCacheEntry<K, V> entry) {
      return new ImmutableInternalCacheEntry<K, V>(entry);
   }

   /**
    * TODO...
    *
    * Creates an immutable version of cache entry whose attributes are set on
    * construction. This is a true shallow copy because referencing the
    * {@link CacheEntry} could lead to changes in the attributes over time,
    * due to its mutability.
    *
    * @param entry the cache entry to provide immutability view for
    * @return an immutable {@link CacheEntry}} wrapper contains the values of
    * the wrapped cache entry at construction time.
    */
   public static <K, V> CacheEntry<K, V> cacheEntryCopy(CacheEntry<K, V> entry) {
      return new CacheEntryCopy<>(entry);
   }

   /**
    * Immutable version of InternalCacheEntry for traversing data containers.
    */
   private static class ImmutableInternalCacheEntry<K, V> implements InternalCacheEntry<K, V>, Immutable {
      private final InternalCacheEntry<K, V> entry;
      private final int hash;

      ImmutableInternalCacheEntry(InternalCacheEntry<K, V> entry) {
         this.entry = entry;
         this.hash = entry.hashCode();
      }

      @Override
      public K getKey() {
         return entry.getKey();
      }

      @Override
      public V getValue() {
         return entry.getValue();
      }

      @Override
      public V setValue(V value) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void commit(DataContainer container, Metadata metadata) {
         throw new UnsupportedOperationException();
      }

      @Override
      @SuppressWarnings("unchecked")
      public boolean equals(Object o) {
         if (!(o instanceof InternalCacheEntry))
            return false;

         InternalCacheEntry entry = (InternalCacheEntry) o;
         return entry.equals(this.entry);
      }

      @Override
      public int hashCode() {
         return hash;
      }

      @Override
      public String toString() {
         return toStr(getKey()) + "=" + toStr(getValue());
      }

      @Override
      public boolean canExpire() {
         return entry.canExpire();
      }

      @Override
      public long getCreated() {
         return entry.getCreated();
      }

      @Override
      public long getExpiryTime() {
         return entry.getExpiryTime();
      }

      @Override
      public long getLastUsed() {
         return entry.getLastUsed();
      }

      @Override
      public boolean isExpired(long now) {
         return entry.isExpired(now);
      }

      @Override
      public boolean isExpired() {
         return entry.isExpired();
      }

      @Override
      public InternalCacheValue<V> toInternalCacheValue() {
         return new CoreImmutables.ImmutableInternalCacheValue(this);
      }

      @Override
      public void touch() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void touch(long currentTimeMillis) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean undelete(boolean doUndelete) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void reincarnate() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void reincarnate(long now) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setMetadata(Metadata metadata) {
         throw new UnsupportedOperationException();
      }

      @Override
      public long getLifespan() {
         return entry.getLifespan();
      }

      @Override
      public long getMaxIdle() {
         return entry.getMaxIdle();
      }

      @Override
      public boolean skipLookup() {
         return false;
      }

      @Override
      public boolean isChanged() {
         return entry.isChanged();
      }

      @Override
      public boolean isCreated() {
         return entry.isCreated();
      }

      @Override
      public boolean isNull() {
         return entry.isNull();
      }

      @Override
      public boolean isRemoved() {
         return entry.isRemoved();
      }

      @Override
      public boolean isEvicted() {
         return entry.isEvicted();
      }

      @Override
      public boolean isValid() {
         return entry.isValid();
      }

      @Override
      public boolean isLoaded() {
         return entry.isLoaded();
      }

      @Override
      public void rollback() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setCreated(boolean created) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setRemoved(boolean removed) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setChanged(boolean changed) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setEvicted(boolean evicted) {
         entry.setEvicted(evicted);
      }

      @Override
      public void setValid(boolean valid) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setLoaded(boolean loaded) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setSkipLookup(boolean skipLookup) {
         throw new UnsupportedOperationException();
      }

      @Override
      public Metadata getMetadata() {
         return entry.getMetadata();
      }

      @Override
      public InternalCacheEntry clone() {
         return new ImmutableInternalCacheEntry(entry.clone());
      }
   }

   private static class ImmutableInternalCacheValue<V> implements InternalCacheValue<V>, Immutable {
      private final ImmutableInternalCacheEntry<?, V> entry;

      ImmutableInternalCacheValue(ImmutableInternalCacheEntry<?, V> entry) {
         this.entry = entry;
      }

      @Override
      public boolean canExpire() {
         return entry.canExpire();
      }

      @Override
      public long getCreated() {
         return entry.getCreated();
      }

      @Override
      public long getLastUsed() {
         return entry.getLastUsed();
      }

      @Override
      public long getLifespan() {
         return entry.getLifespan();
      }

      @Override
      public long getMaxIdle() {
         return entry.getMaxIdle();
      }

      @Override
      public V getValue() {
         return entry.getValue();
      }

      @Override
      public boolean isExpired(long now) {
         return entry.isExpired(now);
      }

      @Override
      public boolean isExpired() {
         return entry.isExpired();
      }

      @Override
      public <K> InternalCacheEntry<K, V> toInternalCacheEntry(K key) {
         return (InternalCacheEntry<K, V>)entry;
      }

      @Override
      public long getExpiryTime() {
         return entry.toInternalCacheValue().getExpiryTime();
      }

      @Override
      public Metadata getMetadata() {
         return entry.getMetadata();
      }
   }

   /**
    * Copy of {@link org.infinispan.container.entries.CacheEntry}.
    */
   private static class CacheEntryCopy<K, V> implements CacheEntry<K, V>, Immutable {
      final int hash;
      final V value;
      final K key;
      final long lifespan;
      final long maxIdle;
      final boolean isChanged;
      final boolean isCreated;
      final boolean isNull;
      final boolean isRemoved;
      final boolean isEvicted;
      final boolean isValid;
      final boolean isLoaded;
      final Metadata metadata;

      CacheEntryCopy(CacheEntry<K, V> entry) {
         metadata = entry.getMetadata();
         key = entry.getKey();
         value = entry.getValue();
         hash = entry.hashCode();
         lifespan = entry.getLifespan();
         maxIdle = entry.getMaxIdle();
         isChanged = entry.isChanged();
         isCreated = entry.isCreated();
         isNull = entry.isNull();
         isRemoved = entry.isRemoved();
         isEvicted = entry.isEvicted();
         isValid = entry.isValid();
         isLoaded = entry.isLoaded();
      }

      @Override
      public K getKey() {
         return key;
      }

      @Override
      public V getValue() {
         return value;
      }

      @Override
      public V setValue(V value) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void commit(DataContainer container, Metadata metadata) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CacheEntry that = (CacheEntry) o;

         if (hash != that.hashCode()) return false;
         if (isChanged != that.isChanged()) return false;
         if (isCreated != that.isCreated()) return false;
         if (isEvicted != that.isEvicted()) return false;
         if (isLoaded != that.isLoaded()) return false;
         if (isNull != that.isNull()) return false;
         if (isRemoved != that.isRemoved()) return false;
         if (isValid != that.isValid()) return false;
         if (lifespan != that.getLifespan()) return false;
         if (maxIdle != that.getMaxIdle()) return false;
         if (!key.equals(that.getKey())) return false;
         if (!metadata.equals(that.getMetadata())) return false;
         if (!value.equals(that.getValue())) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = hash;
         result = 31 * result + value.hashCode();
         result = 31 * result + key.hashCode();
         result = 31 * result + (int) (lifespan ^ (lifespan >>> 32));
         result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
         result = 31 * result + (isChanged ? 1 : 0);
         result = 31 * result + (isCreated ? 1 : 0);
         result = 31 * result + (isNull ? 1 : 0);
         result = 31 * result + (isRemoved ? 1 : 0);
         result = 31 * result + (isEvicted ? 1 : 0);
         result = 31 * result + (isValid ? 1 : 0);
         result = 31 * result + (isLoaded ? 1 : 0);
         result = 31 * result + metadata.hashCode();
         return result;
      }

      @Override
      public boolean undelete(boolean doUndelete) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setMetadata(Metadata metadata) {
         throw new UnsupportedOperationException();
      }

      @Override
      public long getLifespan() {
         return lifespan;
      }

      @Override
      public long getMaxIdle() {
         return maxIdle;
      }

      @Override
      public boolean skipLookup() {
         return false;
      }

      @Override
      public boolean isChanged() {
         return isChanged;
      }

      @Override
      public boolean isCreated() {
         return isCreated;
      }

      @Override
      public boolean isNull() {
         return isNull;
      }

      @Override
      public boolean isRemoved() {
         return isRemoved;
      }

      @Override
      public boolean isEvicted() {
         return isEvicted;
      }

      @Override
      public boolean isValid() {
         return isValid;
      }

      @Override
      public boolean isLoaded() {
         return isLoaded;
      }

      @Override
      public void rollback() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setCreated(boolean created) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setRemoved(boolean removed) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setChanged(boolean changed) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setEvicted(boolean evicted) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setValid(boolean valid) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setLoaded(boolean loaded) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setSkipLookup(boolean skipLookup) {
         throw new UnsupportedOperationException();
      }

      @Override
      public Metadata getMetadata() {
         return metadata;
      }

      @Override
      public String toString() {
         return "CacheEntryCopy{" +
               "hash=" + hash +
               ", value=" + toStr(value) +
               ", key=" + toStr(key) +
               ", lifespan=" + lifespan +
               ", maxIdle=" + maxIdle +
               ", isChanged=" + isChanged +
               ", isCreated=" + isCreated +
               ", isNull=" + isNull +
               ", isRemoved=" + isRemoved +
               ", isEvicted=" + isEvicted +
               ", isValid=" + isValid +
               ", isLoaded=" + isLoaded +
               ", metadata=" + metadata +
               '}';
      }
   }

}

package org.infinispan.util;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commons.util.Immutables;
import org.infinispan.container.DataContainer;
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
   public static InternalCacheEntry immutableInternalCacheEntry(InternalCacheEntry entry) {
      return new ImmutableInternalCacheEntry(entry);
   }

   /**
    * Immutable version of InternalCacheEntry for traversing data containers.
    */
   private static class ImmutableInternalCacheEntry implements InternalCacheEntry, Immutable {
      private final InternalCacheEntry entry;
      private final int hash;

      ImmutableInternalCacheEntry(InternalCacheEntry entry) {
         this.entry = entry;
         this.hash = entry.hashCode();
      }

      @Override
      public Object getKey() {
         return entry.getKey();
      }

      @Override
      public Object getValue() {
         return entry.getValue();
      }

      @Override
      public Object setValue(Object value) {
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
      public InternalCacheValue toInternalCacheValue() {
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
      public boolean skipRemoteGet() {
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
      public void setSkipRemoteGet(boolean skipRemoteGet) {
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

   private static class ImmutableInternalCacheValue implements InternalCacheValue, Immutable {
      private final ImmutableInternalCacheEntry entry;

      ImmutableInternalCacheValue(ImmutableInternalCacheEntry entry) {
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
      public Object getValue() {
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
      public InternalCacheEntry toInternalCacheEntry(Object key) {
         return entry;
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
}

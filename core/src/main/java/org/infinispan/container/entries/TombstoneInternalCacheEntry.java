package org.infinispan.container.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.container.DataContainer;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * An {@link InternalCacheEntry} implementation for tombstone.
 *
 * @since 14.0
 */
public class TombstoneInternalCacheEntry<K, V> implements InternalCacheEntry<K, V> {

   @SuppressWarnings("rawtypes")
   public static final AbstractExternalizer<TombstoneInternalCacheEntry> EXTERNALIZER = new Externalizer();

   private final K key;
   private final PrivateMetadata metadata;

   public TombstoneInternalCacheEntry(K key, PrivateMetadata metadata) {
      this.key = Objects.requireNonNull(key);
      this.metadata = Objects.requireNonNull(metadata);
   }

   @Override
   public boolean isNull() {
      return true;
   }

   @Override
   public boolean isChanged() {
      return false;
   }

   @Override
   public boolean isCreated() {
      return false;
   }

   @Override
   public boolean isRemoved() {
      return true;
   }

   @Override
   public boolean isEvicted() {
      return false;
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public V getValue() {
      return null;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getMaxIdle() {
      return -1;
   }

   @Override
   public boolean skipLookup() {
      return false;
   }

   @Override
   public V setValue(V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void commit(DataContainer<K, V> container) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void setChanged(boolean changed) {
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
   public void setEvicted(boolean evicted) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void setSkipLookup(boolean skipLookup) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isExpired(long now) {
      return false;
   }

   @Override
   public boolean canExpire() {
      return false;
   }

   @Override
   public long getExpiryTime() {
      return -1;
   }

   @Override
   public void touch(long currentTimeMillis) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void reincarnate(long now) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isL1Entry() {
      return false;
   }

   @Override
   public TombstoneInternalCacheValue<V> toInternalCacheValue() {
      return new TombstoneInternalCacheValue<>(metadata);
   }

   @Override
   public InternalCacheEntry<K, V> clone() {
      try {
         //noinspection unchecked
         return (InternalCacheEntry<K, V>) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException();
      }
   }

   @Override
   public Metadata getMetadata() {
      return null;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      throw new UnsupportedOperationException();
   }

   @Override
   public PrivateMetadata getInternalMetadata() {
      return metadata;
   }

   @Override
   public void setCreated(long created) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void setLastUsed(long lastUsed) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void setInternalMetadata(PrivateMetadata metadata) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isTombstone() {
      return true;
   }

   @SuppressWarnings("rawtypes")
   private static final class Externalizer extends AbstractExternalizer<TombstoneInternalCacheEntry> {

      @Override
      public Set<Class<? extends TombstoneInternalCacheEntry>> getTypeClasses() {
         return Collections.singleton(TombstoneInternalCacheEntry.class);
      }

      @Override
      public void writeObject(ObjectOutput output, TombstoneInternalCacheEntry object) throws IOException {
         output.writeObject(object.getKey());
         output.writeObject(object.getInternalMetadata());
      }

      @Override
      public TombstoneInternalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new TombstoneInternalCacheEntry<>(input.readObject(), (PrivateMetadata) input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.TOMBSTONE_INTERNAL_CACHE_ENTRY;
      }
   }

   @Override
   public String toString() {
      return "TombstoneInternalCacheEntry{" +
            "key=" + key +
            ", metadata=" + metadata +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TombstoneInternalCacheEntry)) return false;

      TombstoneInternalCacheEntry<?, ?> that = (TombstoneInternalCacheEntry<?, ?>) o;

      return key.equals(that.key) && metadata.equals(that.metadata);
   }

   @Override
   public int hashCode() {
      int result = key.hashCode();
      result = 31 * result + metadata.hashCode();
      return result;
   }
}

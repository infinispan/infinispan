package org.infinispan.container.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * An {@link InternalCacheValue} implementation for tombstone.
 *
 * @since 14.0
 */
public class TombstoneInternalCacheValue<V> implements InternalCacheValue<V> {

   @SuppressWarnings("rawtypes")
   public static final AbstractExternalizer<TombstoneInternalCacheValue> EXTERNALIZER = new Externalizer();

   private final PrivateMetadata metadata;

   public TombstoneInternalCacheValue(PrivateMetadata metadata) {
      this.metadata = Objects.requireNonNull(metadata);
   }

   @Override
   public V getValue() {
      return null;
   }

   @Override
   public <K> TombstoneInternalCacheEntry<K, V> toInternalCacheEntry(K key) {
      return new TombstoneInternalCacheEntry<>(key, metadata);
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
   public long getCreated() {
      return -1;
   }

   @Override
   public long getLastUsed() {
      return -1;
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
   public long getExpiryTime() {
      return -1;
   }

   @Override
   public Metadata getMetadata() {
      return null;
   }

   @Override
   public PrivateMetadata getInternalMetadata() {
      return metadata;
   }

   @Override
   public void setInternalMetadata(PrivateMetadata internalMetadata) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isTombstone() {
      return true;
   }

   @SuppressWarnings("rawtypes")
   private static final class Externalizer extends AbstractExternalizer<TombstoneInternalCacheValue> {

      @Override
      public Set<Class<? extends TombstoneInternalCacheValue>> getTypeClasses() {
         return Collections.singleton(TombstoneInternalCacheValue.class);
      }

      @Override
      public void writeObject(ObjectOutput output, TombstoneInternalCacheValue object) throws IOException {
         output.writeObject(object.getInternalMetadata());
      }

      @Override
      public TombstoneInternalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new TombstoneInternalCacheValue<>((PrivateMetadata) input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.TOMBSTONE_INTERNAL_CACHE_VALUE;
      }
   }
}

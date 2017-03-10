package org.infinispan.container.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;

/**
 * A cache entry that is transient, i.e., it can be considered expired after a period of not being used.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransientCacheEntry extends AbstractInternalCacheEntry {

   protected Object value;
   protected long maxIdle = -1;
   protected long lastUsed;

   public TransientCacheEntry(Object key, Object value, long maxIdle, long lastUsed) {
      super(key);
      this.value = value;
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
   }

   @Override
   public Object getValue() {
      return value;
   }

   @Override
   public Object setValue(Object value) {
      return this.value = value;
   }

   @Override
   public final void touch() {
      touch(System.currentTimeMillis());
   }

   @Override
   public final void touch(long currentTimeMillis) {
      this.lastUsed = currentTimeMillis;
   }


   @Override
   public final void reincarnate() {
      // no-op
   }

   @Override
   public void reincarnate(long now) {
      // no-op
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransient(maxIdle, lastUsed, now);
   }

   @Override
   public boolean isExpired() {
      return isExpired(System.currentTimeMillis());
   }

   public void setMaxIdle(long maxIdle) {
      this.maxIdle = maxIdle;
   }

   @Override
   public long getCreated() {
      return -1;
   }

   @Override
   public final long getLastUsed() {
      return lastUsed;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getExpiryTime() {
      return maxIdle > -1 ? lastUsed + maxIdle : -1;
   }

   @Override
   public final long getMaxIdle() {
      return maxIdle;
   }

   @Override
   public InternalCacheValue toInternalCacheValue(boolean includeInvocationRecords) {
      return new TransientCacheValue(value, maxIdle, lastUsed);
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder()
            .maxIdle(maxIdle, TimeUnit.MILLISECONDS).build();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      throw new IllegalStateException(
            "Metadata cannot be set on mortal entries. They need to be recreated via the entry factory.");
   }

   @Override
   public TransientCacheEntry clone() {
      return (TransientCacheEntry) super.clone();
   }

   public static class Externalizer extends AbstractExternalizer<TransientCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, TransientCacheEntry tce) throws IOException {
         output.writeObject(tce.key);
         output.writeObject(tce.value);
         UnsignedNumeric.writeUnsignedLong(output, tce.lastUsed);
         output.writeLong(tce.maxIdle); // could be negative so should not use unsigned longs
      }

      @Override
      public TransientCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         Long maxIdle = input.readLong();
         return new TransientCacheEntry(k, v, maxIdle, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.TRANSIENT_ENTRY;
      }

      @Override
      public Set<Class<? extends TransientCacheEntry>> getTypeClasses() {
         return Util.asSet(TransientCacheEntry.class);
      }
   }
}

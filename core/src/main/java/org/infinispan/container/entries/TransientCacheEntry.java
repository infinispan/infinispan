/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.container.entries;

import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.infinispan.util.Util.toStr;

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
   public InternalCacheValue toInternalCacheValue() {
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
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransientCacheEntry that = (TransientCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (value != null ? !value.equals(that.value) : that.value != null)
         return false;
      if (lastUsed != that.lastUsed) return false;
      if (maxIdle != that.maxIdle) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (int) (lastUsed ^ (lastUsed >>> 32));
      result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
      return result;
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
         return Util.<Class<? extends TransientCacheEntry>>asSet(TransientCacheEntry.class);
      }
   }

   @Override
   public String toString() {
      return "TransientCacheEntry{" +
            "key=" + toStr(key) +
            ", value=" + toStr(value) +
            "}";
   }
}

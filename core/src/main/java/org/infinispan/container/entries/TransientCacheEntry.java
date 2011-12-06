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

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * A cache entry that is transient, i.e., it can be considered expired after a period of not being used.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransientCacheEntry extends AbstractInternalCacheEntry {
   private TransientCacheValue cacheValue;

   TransientCacheEntry(Object key, Object value, EntryVersion version, long maxIdle) {
      this(key, value, version, maxIdle, System.currentTimeMillis());
   }

   TransientCacheEntry(Object key, Object value, EntryVersion version, long maxIdle, long lastUsed) {
      super(key);
      cacheValue = new TransientCacheValue(value, version, maxIdle, lastUsed);
   }

   public Object getValue() {
      return cacheValue.value;
   }

   public Object setValue(Object value) {
      return cacheValue.setValue(value);
   }

   @Override
   public EntryVersion getVersion() {
      return cacheValue.version;
   }

   @Override
   public void setVersion(EntryVersion version) {
      this.cacheValue.version = version;
   }

   public final void touch() {
      cacheValue.lastUsed = System.currentTimeMillis();
   }

   public final void reincarnate() {
      // no-op
   }

   public final boolean canExpire() {
      return true;
   }

   public boolean isExpired(long now) {
      return cacheValue.isExpired(now);
   }

   public boolean isExpired() {
      return cacheValue.isExpired();
   }

   public void setMaxIdle(long maxIdle) {
      cacheValue.maxIdle = maxIdle;
   }

   public long getCreated() {
      return -1;
   }

   public final long getLastUsed() {
      return cacheValue.lastUsed;
   }

   public long getLifespan() {
      return -1;
   }

   public long getExpiryTime() {
      return cacheValue.maxIdle > -1 ? cacheValue.lastUsed + cacheValue.maxIdle : -1;
   }

   public final long getMaxIdle() {
      return cacheValue.maxIdle;
   }

   public InternalCacheValue toInternalCacheValue() {
      return cacheValue;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransientCacheEntry that = (TransientCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (cacheValue.value != null ? !cacheValue.value.equals(that.cacheValue.value) : that.cacheValue.value != null)
         return false;
      if (cacheValue.lastUsed != that.cacheValue.lastUsed) return false;
      if (cacheValue.maxIdle != that.cacheValue.maxIdle) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (cacheValue.value != null ? cacheValue.value.hashCode() : 0);
      result = 31 * result + (int) (cacheValue.lastUsed ^ (cacheValue.lastUsed >>> 32));
      result = 31 * result + (int) (cacheValue.maxIdle ^ (cacheValue.maxIdle >>> 32));
      return result;
   }

   @Override
   public TransientCacheEntry clone() {
      TransientCacheEntry clone = (TransientCacheEntry) super.clone();
      clone.cacheValue = cacheValue.clone();
      return clone;
   }

   public static class Externalizer extends AbstractExternalizer<TransientCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, TransientCacheEntry tce) throws IOException {
         output.writeObject(tce.key);
         output.writeObject(tce.cacheValue.value);
         output.writeObject(tce.cacheValue.version);
         UnsignedNumeric.writeUnsignedLong(output, tce.cacheValue.lastUsed);
         output.writeLong(tce.cacheValue.maxIdle); // could be negative so should not use unsigned longs
      }

      @Override
      public TransientCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         EntryVersion version = (EntryVersion) input.readObject();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         Long maxIdle = input.readLong();
         return new TransientCacheEntry(k, v, version, maxIdle, lastUsed);
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
            "key=" + key +
            ", value=" + cacheValue +
            "}";
   }
}

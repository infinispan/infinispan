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

/**
 * A cache entry that is mortal.  I.e., has a lifespan.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MortalCacheEntry extends AbstractInternalCacheEntry {
   protected MortalCacheValue cacheValue;

   protected MortalCacheEntry(Object key, MortalCacheValue cacheValue) {
      super(key);
      this.cacheValue = cacheValue;
   }

   @Override
   public Object getValue() {
      return cacheValue.value;
   }

   @Override
   public Object setValue(Object value) {
      return cacheValue.setValue(value);
   }

   public MortalCacheEntry(Object key, Object value, long lifespan, long created) {
      super(key);
      cacheValue = new MortalCacheValue(value, created, lifespan);
   }

   @Override
   public final boolean isExpired(long now) {
      return cacheValue.isExpired(now);
   }

   @Override
   public final boolean isExpired() {
      return cacheValue.isExpired();
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   public void setLifespan(long lifespan) {
      cacheValue.setLifespan(lifespan);
   }

   @Override
   public final long getCreated() {
      return cacheValue.created;
   }

   @Override
   public final long getLastUsed() {
      return -1;
   }

   @Override
   public final long getLifespan() {
      return cacheValue.lifespan;
   }

   @Override
   public final long getMaxIdle() {
      return -1;
   }

   @Override
   public final long getExpiryTime() {
      return cacheValue.lifespan > -1 ? cacheValue.created + cacheValue.lifespan : -1;
   }

   @Override
   public final void touch() {
      // no-op
   }

   @Override
   public final void touch(long currentTimeMillis) {
      // no-op
   }

   @Override
   public final void reincarnate() {
      reincarnate(System.currentTimeMillis());
   }

   @Override
   public void reincarnate(long now) {
      cacheValue.setCreated(now);
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return cacheValue;
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder().lifespan(cacheValue.getLifespan()).build();
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

      MortalCacheEntry that = (MortalCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (cacheValue.value != null ? !cacheValue.value.equals(that.cacheValue.value) : that.cacheValue.value != null)
         return false;
      if (cacheValue.created != that.cacheValue.created) return false;
      return cacheValue.lifespan == that.cacheValue.lifespan;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (cacheValue.value != null ? cacheValue.value.hashCode() : 0);
      result = 31 * result + (int) (cacheValue.created ^ (cacheValue.created >>> 32));
      result = 31 * result + (int) (cacheValue.lifespan ^ (cacheValue.lifespan >>> 32));
      return result;
   }

   @Override
   public MortalCacheEntry clone() {
      MortalCacheEntry clone = (MortalCacheEntry) super.clone();
      clone.cacheValue = cacheValue.clone();
      return clone;
   }

   public static class Externalizer extends AbstractExternalizer<MortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MortalCacheEntry mce) throws IOException {
         output.writeObject(mce.key);
         output.writeObject(mce.cacheValue.value);
         UnsignedNumeric.writeUnsignedLong(output, mce.cacheValue.created);
         output.writeLong(mce.cacheValue.lifespan); // could be negative so should not use unsigned longs
      }

      @Override
      public MortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Long lifespan = input.readLong();
         return new MortalCacheEntry(k, v, lifespan, created);
      }

      @Override
      public Integer getId() {
         return Ids.MORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends MortalCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends MortalCacheEntry>>asSet(MortalCacheEntry.class);
      }
   }

   @Override
   public String toString() {
      return "MortalCacheEntry{" +
            "key=" + key +
            ", value=" + cacheValue +
            "}";
   }
}

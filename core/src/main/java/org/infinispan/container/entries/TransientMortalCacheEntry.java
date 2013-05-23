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

import static java.lang.Math.min;

/**
 * A cache entry that is both transient and mortal.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransientMortalCacheEntry extends AbstractInternalCacheEntry {

   protected TransientMortalCacheValue cacheValue;

   public TransientMortalCacheEntry(Object key, Object value, long maxIdle, long lifespan) {
      super(key);
      final long currentTimeMillis = System.currentTimeMillis();
      cacheValue = new TransientMortalCacheValue(value, currentTimeMillis, lifespan, maxIdle);
      touch(currentTimeMillis);
   }

   protected TransientMortalCacheEntry(Object key, Object value) {
      super(key);
      final long currentTimeMillis = System.currentTimeMillis();
      cacheValue = new TransientMortalCacheValue(value, currentTimeMillis);
      touch(currentTimeMillis);
   }

   protected TransientMortalCacheEntry(Object key, TransientMortalCacheValue value) {
      super(key);
      this.cacheValue = value;
   }

   public TransientMortalCacheEntry(Object key, Object value, long maxIdle, long lifespan, long lastUsed, long created) {
      super(key);
      this.cacheValue = new TransientMortalCacheValue(value, created, lifespan, maxIdle, lastUsed);
   }

   public void setLifespan(long lifespan) {
      this.cacheValue.lifespan = lifespan;
   }

   public void setMaxIdle(long maxIdle) {
      this.cacheValue.maxIdle = maxIdle;
   }

   @Override
   public Object getValue() {
      return cacheValue.value;
   }

   @Override
   public long getLifespan() {
      return cacheValue.lifespan;
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public long getCreated() {
      return cacheValue.created;
   }

   @Override
   public boolean isExpired(long now) {
      return cacheValue.isExpired(now);
   }

   @Override
   public boolean isExpired() {
      return cacheValue.isExpired();
   }

   @Override
   public final long getExpiryTime() {
      long lset = cacheValue.lifespan > -1 ? cacheValue.created + cacheValue.lifespan : -1;
      long muet = cacheValue.maxIdle > -1 ? cacheValue.lastUsed + cacheValue.maxIdle : -1;
      if (lset == -1) return muet;
      if (muet == -1) return lset;
      return min(lset, muet);
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return cacheValue;
   }

   @Override
   public long getLastUsed() {
      return cacheValue.lastUsed;
   }

   @Override
   public final void touch() {
      cacheValue.lastUsed = System.currentTimeMillis();
   }

   @Override
   public final void touch(long currentTimeMillis) {
      cacheValue.lastUsed = currentTimeMillis;
   }

   @Override
   public final void reincarnate() {
      cacheValue.created = System.currentTimeMillis();
   }

   @Override
   public long getMaxIdle() {
      return cacheValue.maxIdle;
   }

   @Override
   public Object setValue(Object value) {
      return cacheValue.setValue(value);
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder()
            .lifespan(cacheValue.getLifespan())
            .maxIdle(cacheValue.getMaxIdle()).build();
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

      TransientMortalCacheEntry that = (TransientMortalCacheEntry) o;

      if (cacheValue.created != that.cacheValue.created) return false;
      if (cacheValue.lifespan != that.cacheValue.lifespan) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (cacheValue.created ^ (cacheValue.created >>> 32));
      result = 31 * result + (int) (cacheValue.lifespan ^ (cacheValue.lifespan >>> 32));
      return result;
   }

   @Override
   public TransientMortalCacheEntry clone() {
      TransientMortalCacheEntry clone = (TransientMortalCacheEntry) super.clone();
      clone.cacheValue = cacheValue.clone();
      return clone;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "key=" + key +
            ", value=" + cacheValue +
            "}";
   }

   public static class Externalizer extends AbstractExternalizer<TransientMortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, TransientMortalCacheEntry entry) throws IOException {
         output.writeObject(entry.key);
         output.writeObject(entry.cacheValue.value);
         UnsignedNumeric.writeUnsignedLong(output, entry.cacheValue.created);
         output.writeLong(entry.cacheValue.lifespan); // could be negative so should not use unsigned longs
         UnsignedNumeric.writeUnsignedLong(output, entry.cacheValue.lastUsed);
         output.writeLong(entry.cacheValue.maxIdle); // could be negative so should not use unsigned longs
      }

      @Override
      public TransientMortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Long lifespan = input.readLong();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         Long maxIdle = input.readLong();
         return new TransientMortalCacheEntry(k, v, maxIdle, lifespan, lastUsed, created);
      }

      @Override
      public Integer getId() {
         return Ids.TRANSIENT_MORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends TransientMortalCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends TransientMortalCacheEntry>>asSet(TransientMortalCacheEntry.class);
      }
   }
}


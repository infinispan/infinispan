/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.container.entries.metadata;

import org.infinispan.Metadata;
import org.infinispan.container.entries.AbstractInternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import static java.lang.Math.min;

/**
 * A form of {@link org.infinispan.container.entries.TransientMortalCacheEntry}
 * that is {@link org.infinispan.container.entries.versioned.Versioned}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class MetadataTransientMortalCacheEntry extends AbstractInternalCacheEntry implements MetadataAware {

   protected MetadataTransientMortalCacheValue cacheValue;

   public MetadataTransientMortalCacheEntry(Object key, Object value, Metadata metadata) {
      super(key);
      final long currentTimeMillis = System.currentTimeMillis();
      cacheValue = new MetadataTransientMortalCacheValue(value, metadata, currentTimeMillis);
      touch(currentTimeMillis);
   }

   public MetadataTransientMortalCacheEntry(Object key, Object value, Metadata metadata, long lastUsed, long created) {
      super(key);
      this.cacheValue = new MetadataTransientMortalCacheValue(value, metadata, created, lastUsed);
   }

   MetadataTransientMortalCacheEntry(Object key, MetadataTransientMortalCacheValue cacheValue) {
      super(key);
      this.cacheValue = cacheValue;
   }

   @Override
   public Object getValue() {
      return cacheValue.value;
   }

   @Override
   public long getLifespan() {
      return cacheValue.metadata.lifespan();
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
      long lifespan = cacheValue.metadata.lifespan();
      long lset = lifespan > -1 ? cacheValue.created + lifespan : -1;
      long maxIdle = cacheValue.metadata.maxIdle();
      long muet = maxIdle > -1 ? cacheValue.lastUsed + maxIdle : -1;
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
      return cacheValue.metadata.maxIdle();
   }

   @Override
   public Object setValue(Object value) {
      return cacheValue.setValue(value);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataTransientMortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MetadataTransientMortalCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.cacheValue.value);
         output.writeObject(ice.cacheValue.metadata);
         UnsignedNumeric.writeUnsignedLong(output, ice.cacheValue.getCreated());
         UnsignedNumeric.writeUnsignedLong(output, ice.cacheValue.getLastUsed());
      }

      @Override
      public MetadataTransientMortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataTransientMortalCacheEntry(k, v, metadata, lastUsed, created);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_TRANSIENT_MORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends MetadataTransientMortalCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends MetadataTransientMortalCacheEntry>>asSet(MetadataTransientMortalCacheEntry.class);
      }
   }
}

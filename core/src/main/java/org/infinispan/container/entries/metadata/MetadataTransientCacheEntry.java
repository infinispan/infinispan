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

import org.infinispan.metadata.Metadata;
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

/**
 * A cache entry that is transient, i.e., it can be considered expired after
 * a period of not being used, and {@link org.infinispan.container.entries.metadata.MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataTransientCacheEntry extends AbstractInternalCacheEntry implements MetadataAware {

   protected MetadataTransientCacheValue cacheValue;

   public MetadataTransientCacheEntry(Object key, MetadataTransientCacheValue value) {
      super(key);
      this.cacheValue = value;
   }

   public MetadataTransientCacheEntry(Object key, Object value, Metadata metadata) {
      this(key, value, metadata, System.currentTimeMillis());
   }

   public MetadataTransientCacheEntry(Object key, Object value, Metadata metadata, long lastUsed) {
      super(key);
      cacheValue = new MetadataTransientCacheValue(value, metadata, lastUsed);
   }

   @Override
   public Object getValue() {
      return cacheValue.value;
   }

   @Override
   public Object setValue(Object value) {
      return cacheValue.setValue(value);
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
      // no-op
   }

   @Override
   public final boolean canExpire() {
      return true;
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
   public long getCreated() {
      return -1;
   }

   @Override
   public final long getLastUsed() {
      return cacheValue.lastUsed;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getExpiryTime() {
      long maxIdle = cacheValue.metadata.maxIdle();
      return maxIdle > -1 ? cacheValue.lastUsed + maxIdle : -1;
   }

   @Override
   public final long getMaxIdle() {
      return cacheValue.metadata.maxIdle();
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return cacheValue;
   }

   @Override
   public Metadata getMetadata() {
      return cacheValue.getMetadata();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      cacheValue.setMetadata(metadata);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataTransientCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MetadataTransientCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.cacheValue.value);
         output.writeObject(ice.cacheValue.getMetadata());
         UnsignedNumeric.writeUnsignedLong(output, ice.cacheValue.getLastUsed());
      }

      @Override
      public MetadataTransientCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataTransientCacheEntry(k, v, metadata, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_TRANSIENT_ENTRY;
      }

      @Override
      public Set<Class<? extends MetadataTransientCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends MetadataTransientCacheEntry>>asSet(MetadataTransientCacheEntry.class);
      }
   }
}

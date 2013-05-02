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
import org.infinispan.container.entries.ExpiryHelper;
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
 * A cache entry that is mortal and is {@link MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataMortalCacheEntry extends AbstractInternalCacheEntry implements MetadataAware {

   protected MetadataMortalCacheValue cacheValue;

   public MetadataMortalCacheEntry(Object key, Object value, Metadata metadata, long created) {
      super(key);
      cacheValue = new MetadataMortalCacheValue(value, metadata, created);
   }

   MetadataMortalCacheEntry(Object key, MetadataMortalCacheValue cacheValue) {
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

   @Override
   public final boolean isExpired(long now) {
      return ExpiryHelper.isExpiredMortal(
            cacheValue.metadata.lifespan(), cacheValue.created, now);
   }

   @Override
   public final boolean isExpired() {
      return isExpired(System.currentTimeMillis());
   }

   @Override
   public final boolean canExpire() {
      return true;
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
      return cacheValue.metadata.lifespan();
   }

   @Override
   public final long getMaxIdle() {
      return -1;
   }

   @Override
   public final long getExpiryTime() {
      long lifespan = cacheValue.metadata.lifespan();
      return lifespan > -1 ? cacheValue.created + lifespan : -1;
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
      return cacheValue.getMetadata();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      cacheValue.setMetadata(metadata);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataMortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MetadataMortalCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.cacheValue.value);
         output.writeObject(ice.cacheValue.getMetadata());
         UnsignedNumeric.writeUnsignedLong(output, ice.cacheValue.getCreated());
      }

      @Override
      public MetadataMortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataMortalCacheEntry(k, v, metadata, created);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_MORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends MetadataMortalCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends MetadataMortalCacheEntry>>asSet(MetadataMortalCacheEntry.class);
      }
   }
}

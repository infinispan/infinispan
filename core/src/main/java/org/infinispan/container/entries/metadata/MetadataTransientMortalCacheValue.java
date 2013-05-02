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
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * A form of {@link org.infinispan.container.entries.TransientMortalCacheValue} that is {@link org.infinispan.container.entries.versioned.Versioned}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class MetadataTransientMortalCacheValue extends MetadataMortalCacheValue implements MetadataAware {

   long lastUsed;

   public MetadataTransientMortalCacheValue(Object value, Metadata metadata, long created) {
      super(value, metadata, created);
   }

   public MetadataTransientMortalCacheValue(Object v, Metadata metadata, long created, long lastUsed) {
      super(v, metadata, created);
      this.lastUsed = lastUsed;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new MetadataTransientMortalCacheEntry(key, this);
   }

   @Override
   public long getMaxIdle() {
      return metadata.maxIdle();
   }

   @Override
   public long getLastUsed() {
      return lastUsed;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransientMortal(
            metadata.maxIdle(), lastUsed, metadata.lifespan(), created, now);
   }

   @Override
   public boolean isExpired() {
      return isExpired(System.currentTimeMillis());
   }

   public static class Externalizer extends AbstractExternalizer<MetadataTransientMortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, MetadataTransientMortalCacheValue value) throws IOException {
         output.writeObject(value.value);
         output.writeObject(value.metadata);
         UnsignedNumeric.writeUnsignedLong(output, value.created);
         UnsignedNumeric.writeUnsignedLong(output, value.lastUsed);
      }

      @Override
      public MetadataTransientMortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataTransientMortalCacheValue(v, metadata, created, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_TRANSIENT_MORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends MetadataTransientMortalCacheValue>> getTypeClasses() {
         return Util.<Class<? extends MetadataTransientMortalCacheValue>>asSet(MetadataTransientMortalCacheValue.class);
      }
   }

}

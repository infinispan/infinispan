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
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.ImmortalCacheValue;
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
 * A mortal cache value, to correspond with
 * {@link org.infinispan.container.entries.metadata.MetadataMortalCacheEntry}
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class MetadataMortalCacheValue extends ImmortalCacheValue implements MetadataAware {

   Metadata metadata;
   long created;

   public MetadataMortalCacheValue(Object value, Metadata metadata, long created) {
      super(value);
      this.metadata = metadata;
      this.created = created;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new MetadataMortalCacheEntry(key, this);
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public final long getCreated() {
      return created;
   }

   public final void setCreated(long created) {
      this.created = created;
   }

   @Override
   public final long getLifespan() {
      return metadata.lifespan();
   }

   public final void setLifespan(long lifespan) {
      throw new IllegalStateException("Not allowed, use setMetadata instead!");
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredMortal(metadata.lifespan(), created, now);
   }

   @Override
   public boolean isExpired() {
      return ExpiryHelper.isExpiredMortal(metadata.lifespan(), created);
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   public static class Externalizer extends AbstractExternalizer<MetadataMortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, MetadataMortalCacheValue mcv) throws IOException {
         output.writeObject(mcv.value);
         output.writeObject(mcv.metadata);
         UnsignedNumeric.writeUnsignedLong(output, mcv.created);
      }

      @Override
      public MetadataMortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataMortalCacheValue(v, metadata, created);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_MORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends MetadataMortalCacheValue>> getTypeClasses() {
         return Util.<Class<? extends MetadataMortalCacheValue>>asSet(MetadataMortalCacheValue.class);
      }
   }

}

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
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import static org.infinispan.util.Util.toStr;

/**
 * A form of {@link org.infinispan.container.entries.ImmortalCacheValue} that
 * is {@link org.infinispan.container.entries.metadata.MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataImmortalCacheValue extends ImmortalCacheValue implements MetadataAware {

   Metadata metadata;

   public MetadataImmortalCacheValue(Object value, Metadata metadata) {
      super(value);
      this.metadata = metadata;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new MetadataImmortalCacheEntry(key, this);
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
   public String toString() {
      return getClass().getSimpleName() + " {" +
            "value=" + toStr(value) +
            ", metadata=" + metadata +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<MetadataImmortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, MetadataImmortalCacheValue icv) throws IOException {
         output.writeObject(icv.value);
         output.writeObject(icv.metadata);
      }

      @Override
      public MetadataImmortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         return new MetadataImmortalCacheValue(v, metadata);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_IMMORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends MetadataImmortalCacheValue>> getTypeClasses() {
         return Util.<Class<? extends MetadataImmortalCacheValue>>asSet(MetadataImmortalCacheValue.class);
      }
   }

}

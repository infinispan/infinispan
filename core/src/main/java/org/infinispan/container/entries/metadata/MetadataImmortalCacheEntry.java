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
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * A form of {@link org.infinispan.container.entries.ImmortalCacheEntry} that
 * is {@link org.infinispan.container.entries.metadata.MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataImmortalCacheEntry extends ImmortalCacheEntry implements MetadataAware {

   public MetadataImmortalCacheEntry(Object key, Object value, Metadata metadata) {
      super(key, new MetadataImmortalCacheValue(value, metadata));
   }

   MetadataImmortalCacheEntry(Object key, MetadataImmortalCacheValue cacheValue) {
      super(key, cacheValue);
   }

   @Override
   public Metadata getMetadata() {
      return ((MetadataAware) cacheValue).getMetadata();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      ((MetadataAware) cacheValue).setMetadata(metadata);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataImmortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MetadataImmortalCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.cacheValue.value);
         output.writeObject(((MetadataAware) ice.cacheValue).getMetadata());
      }

      @Override
      public MetadataImmortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         return new MetadataImmortalCacheEntry(k, v, metadata);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_IMMORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends MetadataImmortalCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends MetadataImmortalCacheEntry>>asSet(MetadataImmortalCacheEntry.class);
      }
   }
}

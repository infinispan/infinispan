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

import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.metadata.Metadata;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

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

   protected Metadata metadata;

   public MetadataImmortalCacheEntry(Object key, Object value, Metadata metadata) {
      super(key, value);
      this.metadata = metadata;
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
   public InternalCacheValue toInternalCacheValue() {
      return new MetadataImmortalCacheValue(value, metadata);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataImmortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MetadataImmortalCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.value);
         output.writeObject(ice.metadata);
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

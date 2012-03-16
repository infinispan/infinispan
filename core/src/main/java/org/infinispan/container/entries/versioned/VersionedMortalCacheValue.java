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

package org.infinispan.container.entries.versioned;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * A form of {@link MortalCacheValue} that is {@link Versioned}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedMortalCacheValue extends MortalCacheValue implements Versioned {

   EntryVersion version;

   public VersionedMortalCacheValue(Object value, EntryVersion version, long created, long lifespan) {
      super(value, created, lifespan);
      this.version = version;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new VersionedMortalCacheEntry(key, this);
   }

   @Override
   public EntryVersion getVersion() {
      return version;
   }

   @Override
   public void setVersion(EntryVersion version) {
      this.version = version;
   }

   public static class Externalizer extends AbstractExternalizer<VersionedMortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, VersionedMortalCacheValue mcv) throws IOException {
         output.writeObject(mcv.value);
         output.writeObject(mcv.version);
         UnsignedNumeric.writeUnsignedLong(output, mcv.created);
         output.writeLong(mcv.lifespan); // could be negative so should not use unsigned longs
      }

      @Override
      public VersionedMortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         EntryVersion version = (EntryVersion) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Long lifespan = input.readLong();
         return new VersionedMortalCacheValue(v, version, created, lifespan);
      }

      @Override
      public Integer getId() {
         return Ids.VERSIONED_MORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends VersionedMortalCacheValue>> getTypeClasses() {
         return Util.<Class<? extends VersionedMortalCacheValue>>asSet(VersionedMortalCacheValue.class);
      }
   }

}

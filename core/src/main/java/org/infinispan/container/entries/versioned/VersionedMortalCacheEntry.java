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

import org.infinispan.container.entries.MortalCacheEntry;
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
 * A form of {@link MortalCacheEntry} that is {@link Versioned}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedMortalCacheEntry extends MortalCacheEntry implements Versioned {

   public VersionedMortalCacheEntry(Object key, Object value, EntryVersion version, long lifespan, long created) {
      super(key, new VersionedMortalCacheValue(value, version, created, lifespan));
   }

   public VersionedMortalCacheEntry(Object key, Object value, EntryVersion version, long lifespan) {
      this(key, value, version, lifespan, System.currentTimeMillis());
   }

   VersionedMortalCacheEntry(Object key, VersionedMortalCacheValue cacheValue) {
      super(key, cacheValue);
   }

   @Override
   public EntryVersion getVersion() {
      return ((Versioned) cacheValue).getVersion();
   }

   @Override
   public void setVersion(EntryVersion version) {
      ((Versioned) cacheValue).setVersion(version);
   }

   public static class Externalizer extends AbstractExternalizer<VersionedMortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, VersionedMortalCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.cacheValue.value);
         output.writeObject(((Versioned) ice.cacheValue).getVersion());
         UnsignedNumeric.writeUnsignedLong(output, ice.cacheValue.getCreated());
         output.writeLong(ice.cacheValue.getLifespan()); // could be negative so should not use unsigned longs
      }

      @Override
      public VersionedMortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         EntryVersion version = (EntryVersion) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Long lifespan = input.readLong();
         return new VersionedMortalCacheEntry(k, v, version, created, lifespan);
      }

      @Override
      public Integer getId() {
         return Ids.VERSIONED_MORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends VersionedMortalCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends VersionedMortalCacheEntry>>asSet(VersionedMortalCacheEntry.class);
      }
   }
}

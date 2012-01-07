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

import org.infinispan.container.entries.TransientCacheEntry;
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
 * A form of {@link TransientCacheEntry} that is {@link Versioned}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedTransientCacheEntry extends TransientCacheEntry implements Versioned {

   public VersionedTransientCacheEntry(Object key, Object value, EntryVersion version, long maxIdle) {
      this(key, value, version, maxIdle, System.currentTimeMillis());
   }

   public VersionedTransientCacheEntry(Object key, Object value, EntryVersion version, long maxIdle, long lastUsed) {
      super(key, new VersionedTransientCacheValue(value, version, maxIdle, lastUsed));
   }

   VersionedTransientCacheEntry(Object key, VersionedTransientCacheValue cacheValue) {
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

   public static class Externalizer extends AbstractExternalizer<VersionedTransientCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, VersionedTransientCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.cacheValue.value);
         output.writeObject(((Versioned) ice.cacheValue).getVersion());
         UnsignedNumeric.writeUnsignedLong(output, ice.cacheValue.getLastUsed());
         output.writeLong(ice.cacheValue.getMaxIdle()); // could be negative so should not use unsigned longs
      }

      @Override
      public VersionedTransientCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         EntryVersion version = (EntryVersion) input.readObject();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         Long maxIdle = input.readLong();
         return new VersionedTransientCacheEntry(k, v, version, maxIdle, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.VERSIONED_TRANSIENT_ENTRY;
      }

      @Override
      public Set<Class<? extends VersionedTransientCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends VersionedTransientCacheEntry>>asSet(VersionedTransientCacheEntry.class);
      }
   }
}

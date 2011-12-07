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
import org.infinispan.container.entries.TransientMortalCacheValue;
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
 * A form of {@link TransientMortalCacheValue} that is {@link Versioned}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedTransientMortalCacheValue extends TransientMortalCacheValue implements Versioned {

   EntryVersion version;

   public VersionedTransientMortalCacheValue(Object value, EntryVersion version, long created, long lifespan, long maxIdle, long lastUsed) {
      super(value, created, lifespan, maxIdle, lastUsed);
      this.version = version;
   }

   public VersionedTransientMortalCacheValue(Object value, EntryVersion version, long created, long lifespan, long maxIdle) {
      super(value, created, lifespan, maxIdle);
      this.version = version;
   }

   public VersionedTransientMortalCacheValue(Object value, EntryVersion version, long created) {
      super(value, created);
      this.version = version;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new VersionedTransientMortalCacheEntry(key, this);
   }

   @Override
   public EntryVersion getVersion() {
      return version;
   }

   @Override
   public void setVersion(EntryVersion version) {
      this.version = version;
   }

   public static class Externalizer extends AbstractExternalizer<VersionedTransientMortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, VersionedTransientMortalCacheValue value) throws IOException {
         output.writeObject(value.value);
         output.writeObject(value.version);
         UnsignedNumeric.writeUnsignedLong(output, value.created);
         output.writeLong(value.lifespan); // could be negative so should not use unsigned longs
         UnsignedNumeric.writeUnsignedLong(output, value.lastUsed);
         output.writeLong(value.maxIdle); // could be negative so should not use unsigned longs
      }

      @Override
      public VersionedTransientMortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         EntryVersion version = (EntryVersion) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Long lifespan = input.readLong();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         Long maxIdle = input.readLong();
         return new VersionedTransientMortalCacheValue(v, version, created, lifespan, maxIdle, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.VERSIONED_TRANSIENT_MORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends VersionedTransientMortalCacheValue>> getTypeClasses() {
         return Util.<Class<? extends VersionedTransientMortalCacheValue>>asSet(VersionedTransientMortalCacheValue.class);
      }
   }

}

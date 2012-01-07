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
import org.infinispan.container.entries.TransientCacheValue;
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
 * A form of {@link TransientCacheValue} that is {@link Versioned}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedTransientCacheValue extends TransientCacheValue implements Versioned {

   EntryVersion version;

   public VersionedTransientCacheValue(Object value, EntryVersion version, long maxIdle, long lastUsed) {
      super(value, maxIdle, lastUsed);
      this.version = version;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new VersionedTransientCacheEntry(key, this);
   }

   @Override
   public EntryVersion getVersion() {
      return version;
   }

   @Override
   public void setVersion(EntryVersion version) {
      this.version = version;
   }

   public static class Externalizer extends AbstractExternalizer<VersionedTransientCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, VersionedTransientCacheValue tcv) throws IOException {
         output.writeObject(tcv.value);
         output.writeObject(tcv.version);
         UnsignedNumeric.writeUnsignedLong(output, tcv.lastUsed);
         output.writeLong(tcv.maxIdle); // could be negative so should not use unsigned longs
      }

      @Override
      public VersionedTransientCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         EntryVersion version = (EntryVersion) input.readObject();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         Long maxIdle = input.readLong();
         return new VersionedTransientCacheValue(v, version, maxIdle, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.VERSIONED_TRANSIENT_VALUE;
      }

      @Override
      public Set<Class<? extends VersionedTransientCacheValue>> getTypeClasses() {
         return Util.<Class<? extends VersionedTransientCacheValue>>asSet(VersionedTransientCacheValue.class);
      }
   }
}

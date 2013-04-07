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

import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import static org.infinispan.util.Util.toStr;

/**
 * A form of {@link ImmortalCacheValue} that is {@link Versioned}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedImmortalCacheValue extends ImmortalCacheValue implements Versioned {

   EntryVersion version;

   public VersionedImmortalCacheValue(Object value, EntryVersion version) {
      super(value);
      this.version = version;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new VersionedImmortalCacheEntry(key, this);
   }

   @Override
   public EntryVersion getVersion() {
      return version;
   }

   @Override
   public void setVersion(EntryVersion version) {
      this.version = version;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + " {" +
            "value=" + toStr(value) +
            ", version=" + version +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<VersionedImmortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, VersionedImmortalCacheValue icv) throws IOException {
         output.writeObject(icv.value);
         output.writeObject(icv.version);
      }

      @Override
      public VersionedImmortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         EntryVersion version = (EntryVersion) input.readObject();
         return new VersionedImmortalCacheValue(v, version);
      }

      @Override
      public Integer getId() {
         return Ids.VERSIONED_IMMORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends VersionedImmortalCacheValue>> getTypeClasses() {
         return Util.<Class<? extends VersionedImmortalCacheValue>>asSet(VersionedImmortalCacheValue.class);
      }
   }

}

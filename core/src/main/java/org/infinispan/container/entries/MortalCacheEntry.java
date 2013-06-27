/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.container.entries;

import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import static org.infinispan.commons.util.Util.toStr;

/**
 * A cache entry that is mortal.  I.e., has a lifespan.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MortalCacheEntry extends AbstractInternalCacheEntry {

   protected Object value;
   protected long lifespan = -1;
   protected long created;

   public MortalCacheEntry(Object key, Object value, long lifespan, long created) {
      super(key);
      this.value = value;
      this.lifespan = lifespan;
      this.created = created;
   }

   @Override
   public Object getValue() {
      return value;
   }

   @Override
   public Object setValue(Object value) {
      return this.value = value;
   }

   @Override
   public final boolean isExpired(long now) {
      return ExpiryHelper.isExpiredMortal(lifespan, created, now);
   }

   @Override
   public final boolean isExpired() {
      return isExpired(System.currentTimeMillis());
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   public void setLifespan(long lifespan) {
      this.lifespan = lifespan;
   }

   @Override
   public final long getCreated() {
      return created;
   }

   @Override
   public final long getLastUsed() {
      return -1;
   }

   @Override
   public final long getLifespan() {
      return lifespan;
   }

   @Override
   public final long getMaxIdle() {
      return -1;
   }

   @Override
   public final long getExpiryTime() {
      return lifespan > -1 ? created + lifespan : -1;
   }

   @Override
   public final void touch() {
      // no-op
   }

   @Override
   public final void touch(long currentTimeMillis) {
      // no-op
   }

   @Override
   public final void reincarnate() {
      reincarnate(System.currentTimeMillis());
   }

   @Override
   public void reincarnate(long now) {
      this.created = now;
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return new MortalCacheValue(value, created, lifespan);
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder().lifespan(lifespan).build();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      throw new IllegalStateException(
            "Metadata cannot be set on mortal entries. They need to be recreated via the entry factory.");
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MortalCacheEntry that = (MortalCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (value != null ? !value.equals(that.value) : that.value != null)
         return false;
      if (created != that.created) return false;
      return lifespan == that.lifespan;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (int) (created ^ (created >>> 32));
      result = 31 * result + (int) (lifespan ^ (lifespan >>> 32));
      return result;
   }

   @Override
   public MortalCacheEntry clone() {
      return (MortalCacheEntry) super.clone();
   }

   public static class Externalizer extends AbstractExternalizer<MortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MortalCacheEntry mce) throws IOException {
         output.writeObject(mce.key);
         output.writeObject(mce.value);
         UnsignedNumeric.writeUnsignedLong(output, mce.created);
         output.writeLong(mce.lifespan); // could be negative so should not use unsigned longs
      }

      @Override
      public MortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Long lifespan = input.readLong();
         return new MortalCacheEntry(k, v, lifespan, created);
      }

      @Override
      public Integer getId() {
         return Ids.MORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends MortalCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends MortalCacheEntry>>asSet(MortalCacheEntry.class);
      }
   }

   @Override
   public String toString() {
      return "MortalCacheEntry{" +
            "key=" + toStr(key) +
            ", value=" + toStr(value) +
            "}";
   }
}

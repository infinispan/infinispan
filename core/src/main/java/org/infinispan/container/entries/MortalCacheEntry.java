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

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A cache entry that is mortal.  I.e., has a lifespan.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = MortalCacheEntry.Externalizer.class, id = Ids.MORTAL_ENTRY)
public class MortalCacheEntry extends AbstractInternalCacheEntry {
   private MortalCacheValue cacheValue;

   public Object getValue() {
      return cacheValue.value;
   }

   public Object setValue(Object value) {
      return cacheValue.setValue(value);
   }

   MortalCacheEntry(Object key, Object value, long lifespan) {
      this(key, value, lifespan, System.currentTimeMillis());
   }

   MortalCacheEntry(Object key, Object value, long lifespan, long created) {
      super(key);
      cacheValue = new MortalCacheValue(value, created, lifespan);
   }

   public final boolean isExpired() {
      return ExpiryHelper.isExpiredMortal(cacheValue.lifespan, cacheValue.created);
   }

   public final boolean canExpire() {
      return true;
   }

   public void setLifespan(long lifespan) {
      cacheValue.setLifespan(lifespan);
   }

   public final long getCreated() {
      return cacheValue.created;
   }

   public final long getLastUsed() {
      return -1;
   }

   public final long getLifespan() {
      return cacheValue.lifespan;
   }

   public final long getMaxIdle() {
      return -1;
   }

   public final long getExpiryTime() {
      return cacheValue.lifespan > -1 ? cacheValue.created + cacheValue.lifespan : -1;
   }

   public final void touch() {
      // no-op
   }

   public final void reincarnate() {
      cacheValue.created = System.currentTimeMillis();
   }

   public InternalCacheValue toInternalCacheValue() {
      return cacheValue;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MortalCacheEntry that = (MortalCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (cacheValue.value != null ? !cacheValue.value.equals(that.cacheValue.value) : that.cacheValue.value != null)
         return false;
      if (cacheValue.created != that.cacheValue.created) return false;
      return cacheValue.lifespan == that.cacheValue.lifespan;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (cacheValue.value != null ? cacheValue.value.hashCode() : 0);
      result = 31 * result + (int) (cacheValue.created ^ (cacheValue.created >>> 32));
      result = 31 * result + (int) (cacheValue.lifespan ^ (cacheValue.lifespan >>> 32));
      return result;
   }

   @Override
   public MortalCacheEntry clone() {
      MortalCacheEntry clone = (MortalCacheEntry) super.clone();
      clone.cacheValue = cacheValue.clone();
      return clone;
   }

   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         MortalCacheEntry ice = (MortalCacheEntry) subject;
         output.writeObject(ice.key);
         output.writeObject(ice.cacheValue.value);
         UnsignedNumeric.writeUnsignedLong(output, ice.cacheValue.created);
         output.writeLong(ice.cacheValue.lifespan); // could be negative so should not use unsigned longs      
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Long lifespan = input.readLong();
         return new MortalCacheEntry(k, v, lifespan, created);
      }      
   }

   @Override
   public String toString() {
      return "MortalCacheEntry{" +
            "cacheValue=" + cacheValue +
            "} " + super.toString();
   }
}

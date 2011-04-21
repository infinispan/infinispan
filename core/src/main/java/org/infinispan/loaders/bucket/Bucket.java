/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.bucket;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A bucket is where entries are stored.
 */
@Marshallable(externalizer = Bucket.Externalizer.class, id = Ids.BUCKET)
public final class Bucket {
   private Map<Object, InternalCacheEntry> entries = new HashMap<Object, InternalCacheEntry>();
   private transient String bucketName;

   public final void addEntry(InternalCacheEntry se) {
      entries.put(se.getKey(), se);
   }

   public final boolean removeEntry(Object key) {
      return entries.remove(key) != null;
   }

   public final InternalCacheEntry getEntry(Object key) {
      return entries.get(key);
   }

   public final void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      int sz = in.readInt();
      entries = new HashMap<Object, InternalCacheEntry>(sz);
      for (int i = 0; i < sz; i++) {
         InternalCacheEntry se = (InternalCacheEntry) in.readObject();
         entries.put(se.getKey(), se);
      }
   }

   public Map<Object, InternalCacheEntry> getEntries() {
      return entries;
   }

   public String getBucketName() {
      return bucketName;
   }

   public void setBucketName(String bucketName) {
      this.bucketName = bucketName;
   }

   public boolean removeExpiredEntries() {
      boolean result = false;
      Iterator<Map.Entry<Object, InternalCacheEntry>> entryIterator = entries.entrySet().iterator();
      while (entryIterator.hasNext()) {
         Map.Entry<Object, InternalCacheEntry> entry = entryIterator.next();
         if (entry.getValue().isExpired()) {
            entryIterator.remove();
            result = true;
         }
      }
      return result;
   }

   public Collection<? extends InternalCacheEntry> getStoredEntries() {
      return entries.values();
   }

   public long timestampOfFirstEntryToExpire() {
      long result = Long.MAX_VALUE;
      for (InternalCacheEntry se : entries.values()) {
         if (se.getExpiryTime() < result) {
            result = se.getExpiryTime();
         }
      }
      return result;
   }

   @Override
   public String toString() {
      return "Bucket{" +
            "entries=" + entries +
            ", bucketName='" + bucketName + '\'' +
            '}';
   }

   public boolean isEmpty() {
      return entries.isEmpty();
   }

   public int getNumEntries() {
      return entries.size();
   }

   public void clearEntries() {
      entries.clear();
   }
   
   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         Bucket b = (Bucket) subject;
         Map<Object, InternalCacheEntry> entries = b.entries;
         UnsignedNumeric.writeUnsignedInt(output, entries.size());
         for (InternalCacheEntry se : entries.values()) output.writeObject(se);
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Bucket b = new Bucket();
         int numEntries = UnsignedNumeric.readUnsignedInt(input);
         for (int i = 0; i < numEntries; i++) b.addEntry((InternalCacheEntry) input.readObject());
         return b;
      }
   }
}

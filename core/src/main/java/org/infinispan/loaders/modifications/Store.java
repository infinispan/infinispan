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
package org.infinispan.loaders.modifications;

import org.infinispan.container.entries.InternalCacheEntry;

/**
 * Modification representing {@link org.infinispan.loaders.CacheStore#store(org.infinispan.container.entries.InternalCacheEntry)}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class Store implements Modification {

   final InternalCacheEntry storedEntry;

   public Store(InternalCacheEntry storedEntry) {
      this.storedEntry = storedEntry;
   }

   @Override
   public Type getType() {
      return Type.STORE;
   }

   public InternalCacheEntry getStoredEntry() {
      return storedEntry;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Store store = (Store) o;

      if (storedEntry != null ? !storedEntry.equals(store.storedEntry) : store.storedEntry != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return storedEntry != null ? storedEntry.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "Store{" +
            "storedEntry=" + storedEntry +
            '}';
   }
}

/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.loaders.decorators.AsyncStore;

/**
 * Internal null cache entry used to signal that an entry has been removed
 * but it's in the process of being removed from another component,
 * i.e. an async cache store.
 *
 * This entry helps deal with situations where an eventual removal
 * is under way.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class InternalNullEntry implements InternalCacheEntry {

   private final long asyncProcessorId;
   private final AsyncStore asyncStore;

   public InternalNullEntry(AsyncStore asyncStore) {
      this.asyncStore = asyncStore;
      this.asyncProcessorId = this.asyncStore.getAsyncProcessorId();
   }

   @Override
   public boolean isNull() {
      return false; // not null to avoid cache loader interceptor loading from store
   }

   @Override
   public Object getValue() {
      return this; // set this value to avoid cache loader interceptor
   }

   @Override
   public boolean canExpire() {
      return true;
   }

   @Override
   public boolean isExpired(long now) {
      return asyncStore.getAsyncProcessorId() > asyncProcessorId;
   }

   @Override
   public boolean isExpired() {
      return asyncStore.getAsyncProcessorId() > asyncProcessorId;
   }

   // Below are non-relevant method implementations

   @Override
   public boolean isChanged() {
      return false;
   }

   @Override
   public boolean isCreated() {
      return false;
   }

   @Override
   public boolean isRemoved() {
      return false;
   }

   @Override
   public boolean isEvicted() {
      return false;
   }

   @Override
   public boolean isValid() {
      return false;
   }

   @Override
   public Object getKey() {
      return null;
   }

   @Override
   public long getLifespan() {
      return 0;
   }

   @Override
   public long getMaxIdle() {
      return 0;
   }

   @Override
   public void setMaxIdle(long maxIdle) {
      // Empty
   }

   @Override
   public void setLifespan(long lifespan) {
      // Empty
   }

   @Override
   public Object setValue(Object value) {
      return null;
   }

   @Override
   public boolean equals(Object o) {
      return false;
   }

   @Override
   public int hashCode() {
      return 0;
   }

   @Override
   public void commit(DataContainer container, EntryVersion newVersion) {
      // Empty
   }

   @Override
   public void rollback() {
      // Empty
   }

   @Override
   public void setCreated(boolean created) {
      // Empty
   }

   @Override
   public void setRemoved(boolean removed) {
      // Empty
   }

   @Override
   public void setEvicted(boolean evicted) {
      // Empty
   }

   @Override
   public void setValid(boolean valid) {
      // Empty
   }

   @Override
   public boolean isLockPlaceholder() {
      return false;
   }

   @Override
   public boolean undelete(boolean doUndelete) {
      return false;
   }

   @Override
   public long getCreated() {
      return 0;
   }

   @Override
   public long getLastUsed() {
      return 0;
   }

   @Override
   public long getExpiryTime() {
      return 0;
   }

   @Override
   public void touch() {
      // Empty
   }

   @Override
   public void touch(long currentTimeMillis) {
      // Empty
   }

   @Override
   public void reincarnate() {
      // Empty
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return null;
   }

   @Override
   public InternalCacheEntry clone() {
      return null;
   }

   @Override
   public EntryVersion getVersion() {
      return null;
   }

   @Override
   public void setVersion(EntryVersion version) {
      // Empty
   }

}

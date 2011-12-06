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

import org.infinispan.container.DataContainer;
import org.infinispan.container.versioning.EntryVersion;

/**
 * An abstract internal cache entry that is typically stored in the data container
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractInternalCacheEntry implements InternalCacheEntry {

   Object key;

   AbstractInternalCacheEntry() {
   }

   AbstractInternalCacheEntry(Object key) {
      this.key = key;
   }

   public final void commit(DataContainer container, EntryVersion newVersion) {
      // no-op
   }

   public final void rollback() {
      // no-op
   }

   public final void setCreated(boolean created) {
      // no-op
   }

   public final void setRemoved(boolean removed) {
      // no-op
   }

   public final void setEvicted(boolean evicted) {
      // no-op
   }

   public final void setValid(boolean valid) {
      // no-op
   }

   public final boolean isNull() {
      return false;
   }

   public final boolean isChanged() {
      return false;
   }

   public final boolean isCreated() {
      return false;
   }

   public final boolean isRemoved() {
      return false;
   }

   public final boolean isEvicted() {
      return true;
   }

   public final boolean isValid() {
      return false;
   }

   @Override
   public boolean undelete(boolean doUndelete) {
      return false;
   }

   public final boolean isLockPlaceholder() {
      return false;
   }

   public void setMaxIdle(long maxIdle) {
   }

   public void setLifespan(long lifespan) {
   }

   public final Object getKey() {
      return key;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "key=" + key +
            '}';
   }

   public AbstractInternalCacheEntry clone() {
      try {
         return (AbstractInternalCacheEntry) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen!", e);
      }
   }
}

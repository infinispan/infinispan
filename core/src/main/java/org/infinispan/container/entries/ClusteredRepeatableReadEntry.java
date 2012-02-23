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

package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;

/**
 * A version of RepeatableReadEntry that can perform write-skew checks during prepare.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class ClusteredRepeatableReadEntry extends RepeatableReadEntry {
   private EntryVersion version;

   //Pedro -- total order: identifies this entry as read
   private boolean read;
   //Pedro -- total order: mark this entries to do the write skew check in commit time (if write skew is enabled)
   private boolean writeSkewInCommitTime;

   public ClusteredRepeatableReadEntry(Object key, Object value, EntryVersion version, long lifespan) {
      super(key, value, version, lifespan);
      this.version = version;
   }

   public boolean performWriteSkewCheck(DataContainer container) {
      InternalCacheEntry ice = container.get(key);
      if (ice == null) return version == null;
      if (ice.getVersion() == null)
         throw new IllegalStateException("Entries cannot have null versions!");
      // Could be that we didn't do a remote get first ... so we haven't effectively read this entry yet.
      if (version == null) return true;
      return InequalVersionComparisonResult.AFTER != ice.getVersion().compareTo(version);
   }

   @Override
   public EntryVersion getVersion() {
      return version;
   }

   @Override
   public void setVersion(EntryVersion version) {
      this.version = version;
   }

   //Pedro -- total order stuff..

   @Override
   public void copyForUpdate(DataContainer container, boolean localModeWriteSkewCheck) {
      if(!isChanged()) {
         //Pedro -- this entry needs the write skew check if, in write time, it was previously read
         writeSkewInCommitTime = read;
      }
      super.copyForUpdate(container, localModeWriteSkewCheck);    //To change body of overridden methods use File | Settings | File Templates.
   }

   /**
    * mark this entry as read
    */
   public void markAsRead() {
      read = true;
   }

   /**
    * check is this entry is marked for write skew in commit time
    * @return true if the write skew must be performed, false otherwise
    */
   public boolean isMarkedForWriteSkew() {
      return writeSkewInCommitTime;
   }

   /**
    * mark this entry for write skew in commit time
    */
   public void markForWriteSkewCheck() {
      writeSkewInCommitTime = true;
   }
}

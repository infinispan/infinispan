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

package org.infinispan.container.versioning;

import net.jcip.annotations.Immutable;

import java.io.Serializable;

/**
 * A simple versioning scheme that is cluster-aware
 *
 * @author Manik Surtani
 * @since 5.1
 */
@Immutable
public class SimpleClusteredVersion implements IncrementableEntryVersion, Serializable {
   private final int viewId;
   final long version;

   public SimpleClusteredVersion(int viewId, long version) {
      this.version = version;
      this.viewId = viewId;
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      if (other instanceof SimpleClusteredVersion) {
         SimpleClusteredVersion otherVersion = (SimpleClusteredVersion) other;

         if (viewId > otherVersion.viewId)
            return InequalVersionComparisonResult.AFTER;
         if (viewId < otherVersion.viewId)
            return InequalVersionComparisonResult.BEFORE;

         if (version > otherVersion.version)
            return InequalVersionComparisonResult.AFTER;
         if (version < otherVersion.version)
            return InequalVersionComparisonResult.BEFORE;

         return InequalVersionComparisonResult.EQUAL;
      } else {
         throw new IllegalArgumentException("I only know how to deal with SimpleClusteredVersions, not " + other.getClass().getName());
      }
   }

   @Override
   public String toString() {
      return "SimpleClusteredVersion{" +
            "viewId=" + viewId +
            ", version=" + version +
            '}';
   }
}

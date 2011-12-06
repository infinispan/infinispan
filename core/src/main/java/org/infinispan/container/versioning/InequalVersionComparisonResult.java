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

/**
 * Versions can be compared to each other to result in one version being before, after or at the same time as another
 * version.  This is different from the JDK's {@link Comparable} interface, which is much more simplistic in that it
 * doesn't differentiate between something that is the same versus equal-but-different.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public enum InequalVersionComparisonResult {
   /**
    * Denotes a version that was created temporally <i>before</i> another version.
    */
   BEFORE,
   /**
    * Denotes a version that was created temporally <i>after</i> another version.
    */
   AFTER,
   /**
    * Denotes that the two versions being comapred are equal.
    */
   EQUAL,
   /**
    * Denotes a version that was created at the same time as another version, but is not equal.  This is only really
    * useful when using a partition-aware versioning scheme, such as vector or Lamport clocks.
    */
   CONFLICTING
}

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

package org.infinispan.test.fwk;

/**
 * Flags that allow JGroups transport stack to be tweaked depending on the test
 * case requirements. For example, you can remove failure detection, or remove
 * merge protocol...etc.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class TransportFlags {

   boolean withFD;
   boolean withMerge;

   public TransportFlags withFD(boolean withFD) {
      this.withFD = withFD;
      return this;
   }

   public boolean withFD() {
      return withFD;
   }

   public TransportFlags withMerge(boolean withMerge) {
      this.withMerge = withMerge;
      return this;
   }

   public boolean withMerge() {
      return withMerge;
   }

}

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
package org.infinispan.configuration.cache;

/**
 * Configures deadlock detection.
 */
public class DeadlockDetectionConfiguration {

   private final boolean enabled;
   private final long spinDuration;
   
   DeadlockDetectionConfiguration(boolean enabled, long spinDuration) {
      this.enabled = enabled;
      this.spinDuration = spinDuration;
   }
   
   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    */
   public long spinDuration() {
      return spinDuration;
   }
   
   /**
    * Whether deadlock detection is enabled or disabled
    */
   public boolean enabled() {
      return enabled;
   }

   @Override
   public String toString() {
      return "DeadlockDetectionConfiguration{" +
            "enabled=" + enabled +
            ", spinDuration=" + spinDuration +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DeadlockDetectionConfiguration that = (DeadlockDetectionConfiguration) o;

      if (enabled != that.enabled) return false;
      if (spinDuration != that.spinDuration) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + (int) (spinDuration ^ (spinDuration >>> 32));
      return result;
   }

}

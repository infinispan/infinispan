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
 * This configuration element controls whether entries are versioned.  Versioning is necessary, for example, when
 * using optimistic transactions in a clustered environment, to be able to perform write-skew checks.
 */
public class VersioningConfiguration {
   private final boolean enabled;
   private final VersioningScheme scheme;

   VersioningConfiguration(boolean enabled, VersioningScheme scheme) {
      this.enabled = enabled;
      this.scheme = scheme;
   }

   public boolean enabled() {
      return enabled;
   }

   public VersioningScheme scheme() {
      return scheme;
   }

   @Override
   public String toString() {
      return "VersioningConfiguration{" +
            "enabled=" + enabled +
            ", scheme=" + scheme +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      VersioningConfiguration that = (VersioningConfiguration) o;

      if (enabled != that.enabled) return false;
      if (scheme != that.scheme) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + (scheme != null ? scheme.hashCode() : 0);
      return result;
   }

}

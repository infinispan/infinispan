/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.configuration.cache;

import org.infinispan.marshall.Marshaller;

/**
 * Compatibility mode configuration
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class CompatibilityModeConfiguration {

   private final boolean enabled;
   private final Marshaller marshaller;

   public CompatibilityModeConfiguration(boolean enabled, Marshaller marshaller) {
      this.enabled = enabled;
      this.marshaller = marshaller;
   }

   public boolean enabled() {
      return enabled;
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   @Override
   public String toString() {
      return "CompatibilityModeConfiguration{" +
            "enabled=" + enabled +
            ", marshaller=" + marshaller +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CompatibilityModeConfiguration that = (CompatibilityModeConfiguration) o;

      if (enabled != that.enabled) return false;
      if (marshaller != null ? !marshaller.equals(that.marshaller) : that.marshaller != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + (marshaller != null ? marshaller.hashCode() : 0);
      return result;
   }
}

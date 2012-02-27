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

 * Controls certain tuning parameters that may break some of Infinispan's public API contracts in exchange for better
 * performance in some cases.
 * <p />
 * Use with care, only after thoroughly reading and understanding the documentation about a specific feature.
 * <p />
 * @see UnsafeConfigurationBuilder
 */
public class UnsafeConfiguration {

   private final boolean unreliableReturnValues;

   UnsafeConfiguration(boolean unreliableReturnValues) {
      this.unreliableReturnValues = unreliableReturnValues;
   }

   /**
    * Specifies whether Infinispan is allowed to disregard the {@link Map} contract when providing return values for
    * {@link org.infinispan.Cache#put(Object, Object)} and {@link org.infinispan.Cache#remove(Object)} methods.
    */
   public boolean unreliableReturnValues() {
      return unreliableReturnValues;
   }

   @Override
   public String toString() {
      return "UnsafeConfiguration{" +
            "unreliableReturnValues=" + unreliableReturnValues +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      UnsafeConfiguration that = (UnsafeConfiguration) o;

      if (unreliableReturnValues != that.unreliableReturnValues) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return (unreliableReturnValues ? 1 : 0);
   }

}

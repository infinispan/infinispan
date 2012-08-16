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

import org.infinispan.loaders.CacheLoader;
import org.infinispan.util.TypedProperties;

/**
 * Configuration a legacy cache loader or cache store, i.e. one which doesn't provide its own builder
 *
 * @author Pete Muir
 * @since 5.1
 */
public class LegacyLoaderConfiguration extends AbstractLoaderConfiguration {

   private final CacheLoader cacheLoader;

   LegacyLoaderConfiguration(TypedProperties properties, CacheLoader cacheLoader) {
      super(properties);
      this.cacheLoader = cacheLoader;
   }

   public CacheLoader cacheLoader() {
      return cacheLoader;
   }

   @Override
   public String toString() {
      return "LoaderConfiguration{" +
            "cacheLoader=" + cacheLoader +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      LegacyLoaderConfiguration that = (LegacyLoaderConfiguration) o;

      if (cacheLoader != null ? !cacheLoader.equals(that.cacheLoader) : that.cacheLoader != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (cacheLoader != null ? cacheLoader.hashCode() : 0);
      return result;
   }

}

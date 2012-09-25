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

import java.util.Properties;

/*
 * This is slightly different AbstractLoaderConfigurationChildBuilder, as it instantiates a new set of children (async and singletonStore)
 * rather than delegate to existing ones.
 */
public abstract class AbstractLoaderConfigurationBuilder<T extends LoaderConfiguration, S extends AbstractLoaderConfigurationBuilder<T, S>> extends
      AbstractLoadersConfigurationChildBuilder implements LoaderConfigurationBuilder<T, S> {
   protected Properties properties = new Properties();

   public AbstractLoaderConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * <p>
    * Defines a single property. Can be used multiple times to define all needed properties, but the
    * full set is overridden by {@link #withProperties(Properties)}.
    * </p>
    * <p>
    * These properties are passed directly to the cache store.
    * </p>
    */
   @Override
   public S addProperty(String key, String value) {
      this.properties.put(key, value);
      return self();
   }

   /**
    * <p>
    * These properties are passed directly to the cache store.
    * </p>
    */
   @Override
   public S withProperties(Properties props) {
      this.properties = props;
      return self();
   }
}

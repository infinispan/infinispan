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

/**
 *
 * AbstractLoaderConfigurationChildBuilder delegates {@link LoaderConfigurationChildBuilder} methods to a specified {@link LoaderConfigurationBuilder}
 *
 * @author Pete Muir
 * @author Tristan Tarrant
 * @since 5.1
 */
public abstract class AbstractLoaderConfigurationChildBuilder<S> extends AbstractLoadersConfigurationChildBuilder implements LoaderConfigurationChildBuilder<S> {

   private final LoaderConfigurationBuilder<? extends AbstractLoaderConfiguration, ? extends LoaderConfigurationBuilder<?,?>> builder;

   protected AbstractLoaderConfigurationChildBuilder(LoaderConfigurationBuilder<? extends AbstractLoaderConfiguration, ? extends LoaderConfigurationBuilder<?,?>> builder) {
      super(builder.loaders());
      this.builder = builder;
   }

   @Override
   public S addProperty(String key, String value) {
      return (S)builder.addProperty(key, value);
   }

   @Override
   public S withProperties(Properties p) {
      return (S)builder.withProperties(p);
   }
}

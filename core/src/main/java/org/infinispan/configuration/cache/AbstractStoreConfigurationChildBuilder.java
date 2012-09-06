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

public abstract class AbstractStoreConfigurationChildBuilder<T> extends AbstractLoaderConfigurationChildBuilder<T> implements StoreConfigurationChildBuilder {

   private final StoreConfigurationBuilder<? extends AbstractStoreConfiguration, ? extends StoreConfigurationBuilder<?,?>> storeConfigurationBuilder;

   protected AbstractStoreConfigurationChildBuilder(AbstractStoreConfigurationBuilder<? extends AbstractStoreConfiguration, ?> builder) {
      super(builder.loaders());
      this.storeConfigurationBuilder = builder;
   }

   @Override
   public AsyncStoreConfigurationBuilder async() {
      return storeConfigurationBuilder.async();
   }

   @Override
   public SingletonStoreConfigurationBuilder singletonStore() {
      return storeConfigurationBuilder.singletonStore();
   }

}

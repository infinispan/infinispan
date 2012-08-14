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
public abstract class AbstractLoaderConfigurationBuilder<T extends AbstractLoaderConfiguration> extends
      AbstractLoadersConfigurationChildBuilder<T> {

   protected final AsyncStoreConfigurationBuilder async;
   protected final SingletonStoreConfigurationBuilder singletonStore;

   public AbstractLoaderConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      this.async = new AsyncStoreConfigurationBuilder(this);
      this.singletonStore = new SingletonStoreConfigurationBuilder(this);
   }

   /**
    * Configuration for the async cache loader. If enabled, this provides you with asynchronous
    * writes to the cache store, giving you 'write-behind' caching.
    */
   public AsyncStoreConfigurationBuilder async() {
      return async;
   }

   /**
    * SingletonStore is a delegating cache store used for situations when only one instance in a
    * cluster should interact with the underlying store. The coordinator of the cluster will be
    * responsible for the underlying CacheStore. SingletonStore is a simply facade to a real
    * CacheStore implementation. It always delegates reads to the real CacheStore.
    */
   public SingletonStoreConfigurationBuilder singletonStore() {
      return singletonStore;
   }

   /**
    * Properties passed to the cache store or loader
    * @param p
    * @return
    */
   public abstract AbstractLoaderConfigurationBuilder<T> withProperties(Properties p);

}
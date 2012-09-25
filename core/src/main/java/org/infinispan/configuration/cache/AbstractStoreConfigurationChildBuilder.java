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
*
* AbstractStoreConfigurationChildBuilder delegates {@link StoreConfigurationChildBuilder} methods to a specified {@link StoreConfigurationBuilder}
*
* @author Tristan Tarrant
* @since 5.2
*/
public abstract class AbstractStoreConfigurationChildBuilder<S> extends AbstractLoaderConfigurationChildBuilder<S> implements StoreConfigurationChildBuilder<S> {

   private final StoreConfigurationBuilder<? extends AbstractStoreConfiguration, ? extends StoreConfigurationBuilder<?, ?>> builder;

   protected AbstractStoreConfigurationChildBuilder(AbstractStoreConfigurationBuilder<? extends AbstractStoreConfiguration, ?> builder) {
      super(builder);
      this.builder = builder;
   }

   @Override
   public AsyncStoreConfigurationBuilder<S> async() {
      return (AsyncStoreConfigurationBuilder<S>) builder.async();
   }

   @Override
   public SingletonStoreConfigurationBuilder<S> singletonStore() {
      return (SingletonStoreConfigurationBuilder<S>) builder.singletonStore();
   }

   @Override
   public S fetchPersistentState(boolean b) {
      return (S) builder.fetchPersistentState(b);
   }

   @Override
   public S ignoreModifications(boolean b) {
      return (S) builder.ignoreModifications(b);
   }

   @Override
   public S purgeOnStartup(boolean b) {
      return (S) builder.purgeOnStartup(b);
   }

   @Override
   public S purgerThreads(int i) {
      return (S) builder.purgerThreads(i);
   }

   @Override
   public S purgeSynchronously(boolean b) {
      return (S) builder.purgeSynchronously(b);
   }

}

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
 * Defines clustered characteristics of the cache.
 * 
 * @author pmuir
 * 
 */
public class ClusteringConfigurationBuilder extends AbstractConfigurationChildBuilder<ClusteringConfiguration> implements
      ClusteringConfigurationChildBuilder {

   private CacheMode cacheMode = CacheMode.LOCAL;
   private final AsyncConfigurationBuilder asyncConfigurationBuilder;
   private final HashConfigurationBuilder hashConfigurationBuilder;
   private final L1ConfigurationBuilder l1ConfigurationBuilder;
   private final StateTransferConfigurationBuilder stateTransferConfigurationBuilder;
   private final SyncConfigurationBuilder syncConfigurationBuilder;

   ClusteringConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.asyncConfigurationBuilder = new AsyncConfigurationBuilder(this);
      this.hashConfigurationBuilder = new HashConfigurationBuilder(this);
      this.l1ConfigurationBuilder = new L1ConfigurationBuilder(this);
      this.stateTransferConfigurationBuilder = new StateTransferConfigurationBuilder(this);
      this.syncConfigurationBuilder = new SyncConfigurationBuilder(this);
   }

   /**
    * Cache mode. See {@link CacheMode} for information on the various cache modes available.
    */
   public ClusteringConfigurationBuilder cacheMode(CacheMode cacheMode) {
      this.cacheMode = cacheMode;
      return this;
   }

   CacheMode cacheMode() {
      return cacheMode;
   }

   /**
    * Configure async sub element. Once this method is invoked users cannot subsequently invoke
    * <code>configureSync()</code> as two are mutually exclusive
    */
   @Override
   public AsyncConfigurationBuilder async() {
      if (cacheMode.isSynchronous())
         throw new IllegalStateException("Cannot configure a async for an sync cache. Set the cache mode to async first.");
      return asyncConfigurationBuilder;
   }

   /**
    * Configure hash sub element
    */
   @Override
   public HashConfigurationBuilder hash() {
      return hashConfigurationBuilder;
   }

   /**
    * This method allows configuration of the L1 cache for distributed
    * caches. When this method is called, it automatically enables L1. So,
    * if you want it to be disabled, make sure you call
    * {@link org.infinispan.configuration.cache.L1ConfigurationBuilder#disable()}
    */
   @Override
   public L1ConfigurationBuilder l1() {
      return l1ConfigurationBuilder;
   }

   /**
    * Configure sync sub element. Once this method is invoked users cannot subsequently invoke
    * <code>configureAsync()</code> as two are mutually exclusive
    */
   @Override
   public StateTransferConfigurationBuilder stateTransfer() {
      return stateTransferConfigurationBuilder;
   }

   @Override
   public SyncConfigurationBuilder sync() {
      if (!cacheMode.isSynchronous())
         throw new IllegalStateException("Cannot configure a sync for an async cache. Set the cache mode to sync first.");
      return syncConfigurationBuilder;
   }

   @Override
   void validate() {
      asyncConfigurationBuilder.validate();
      hashConfigurationBuilder.validate();
      l1ConfigurationBuilder.validate();
      syncConfigurationBuilder.validate();

   }

   @Override
   ClusteringConfiguration create() {
      return new ClusteringConfiguration(cacheMode, asyncConfigurationBuilder.create(), hashConfigurationBuilder.create(),
            l1ConfigurationBuilder.create(), stateTransferConfigurationBuilder.create(), syncConfigurationBuilder.create());
   }

   @Override
   public ClusteringConfigurationBuilder read(ClusteringConfiguration template) {
      this.cacheMode = template.cacheMode();
      asyncConfigurationBuilder.read(template.async());
      hashConfigurationBuilder.read(template.hash());
      l1ConfigurationBuilder.read(template.l1());
      stateTransferConfigurationBuilder.read(template.stateTransfer());
      syncConfigurationBuilder.read(template.sync());

      return this;
   }

}

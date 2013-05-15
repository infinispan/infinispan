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

abstract class AbstractConfigurationChildBuilder implements ConfigurationChildBuilder {

   private final ConfigurationBuilder builder;

   protected AbstractConfigurationChildBuilder(ConfigurationBuilder builder) {
      this.builder = builder;
   }

   @Override
   public ClusteringConfigurationBuilder clustering() {
      return builder.clustering();
   }

   @Override
   public CustomInterceptorsConfigurationBuilder customInterceptors() {
      return builder.customInterceptors();
   }

   @Override
   public DataContainerConfigurationBuilder dataContainer() {
      return builder.dataContainer();
   }

   @Override
   public DeadlockDetectionConfigurationBuilder deadlockDetection() {
      return builder.deadlockDetection();
   }

   @Override
   public EvictionConfigurationBuilder eviction() {
      return builder.eviction();
   }

   @Override
   public ExpirationConfigurationBuilder expiration() {
      return builder.expiration();
   }

   @Override
   public IndexingConfigurationBuilder indexing() {
      return builder.indexing();
   }

   @Override
   public InvocationBatchingConfigurationBuilder invocationBatching() {
      return builder.invocationBatching();
   }

   @Override
   public JMXStatisticsConfigurationBuilder jmxStatistics() {
      return builder.jmxStatistics();
   }

   @Override
   public LoadersConfigurationBuilder loaders() {
      return builder.loaders();
   }

   @Override
   public LockingConfigurationBuilder locking() {
      return builder.locking();
   }

   @Override
   public StoreAsBinaryConfigurationBuilder storeAsBinary() {
      return builder.storeAsBinary();
   }

   @Override
   public TransactionConfigurationBuilder transaction() {
      return builder.transaction();
   }

   @Override
   public VersioningConfigurationBuilder versioning() {
     return builder.versioning();
   }

   @Override
   public UnsafeConfigurationBuilder unsafe() {
      return builder.unsafe();
   }

   @Override
   public SitesConfigurationBuilder sites() {
      return builder.sites();
   }

   @Override
   public CompatibilityModeConfigurationBuilder compatibility() {
      return builder.compatibility();
   }

   protected ConfigurationBuilder getBuilder() {
      return builder;
   }

   @Override
   public Configuration build() {
      return builder.build();
   }

}
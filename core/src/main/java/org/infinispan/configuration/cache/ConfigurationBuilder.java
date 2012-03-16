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

import static java.util.Arrays.asList;

public class ConfigurationBuilder implements ConfigurationChildBuilder {

   private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
   private final ClusteringConfigurationBuilder clustering;
   private final CustomInterceptorsConfigurationBuilder customInterceptors;
   private final DataContainerConfigurationBuilder dataContainer;
   private final DeadlockDetectionConfigurationBuilder deadlockDetection;
   private final EvictionConfigurationBuilder eviction;
   private final ExpirationConfigurationBuilder expiration;
   private final IndexingConfigurationBuilder indexing;
   private final InvocationBatchingConfigurationBuilder invocationBatching;
   private final JMXStatisticsConfigurationBuilder jmxStatistics;
   private final LoadersConfigurationBuilder loaders;
   private final LockingConfigurationBuilder locking;
   private final StoreAsBinaryConfigurationBuilder storeAsBinary;
   private final TransactionConfigurationBuilder transaction;
   private final VersioningConfigurationBuilder versioning;
   private final UnsafeConfigurationBuilder unsafe;
   
   public ConfigurationBuilder() {
      this.clustering = new ClusteringConfigurationBuilder(this);
      this.customInterceptors = new CustomInterceptorsConfigurationBuilder(this);
      this.dataContainer = new DataContainerConfigurationBuilder(this);
      this.deadlockDetection = new DeadlockDetectionConfigurationBuilder(this);
      this.eviction = new EvictionConfigurationBuilder(this);
      this.expiration = new ExpirationConfigurationBuilder(this);
      this.indexing = new IndexingConfigurationBuilder(this);
      this.invocationBatching = new InvocationBatchingConfigurationBuilder(this);
      this.jmxStatistics = new JMXStatisticsConfigurationBuilder(this);
      this.loaders = new LoadersConfigurationBuilder(this);
      this.locking = new LockingConfigurationBuilder(this);
      this.storeAsBinary = new StoreAsBinaryConfigurationBuilder(this);
      this.transaction = new TransactionConfigurationBuilder(this);
      this.versioning = new VersioningConfigurationBuilder(this);
      this.unsafe = new UnsafeConfigurationBuilder(this);
   }
   
   public ConfigurationBuilder classLoader(ClassLoader cl) {
      this.classLoader = cl;
      return this;
   }
   
   ClassLoader classLoader() {
      return classLoader;
   }

   @Override
   public ClusteringConfigurationBuilder clustering() {
      return clustering;
   }
   
   @Override
   public CustomInterceptorsConfigurationBuilder customInterceptors() {
      return customInterceptors;
   }
   
   @Override
   public DataContainerConfigurationBuilder dataContainer() {
      return dataContainer;
   } 
   
   @Override
   public DeadlockDetectionConfigurationBuilder deadlockDetection() {
      return deadlockDetection;
   }
   
   @Override
   public EvictionConfigurationBuilder eviction() {
      return eviction;
   }
   
   @Override
   public ExpirationConfigurationBuilder expiration() {
      return expiration;
   }
   
   @Override
   public IndexingConfigurationBuilder indexing() {
      return indexing;
   }
   
   @Override
   public InvocationBatchingConfigurationBuilder invocationBatching() {
      return invocationBatching;
   }
   
   @Override
   public JMXStatisticsConfigurationBuilder jmxStatistics() {
      return jmxStatistics;
   }
   
   @Override
   public StoreAsBinaryConfigurationBuilder storeAsBinary() {
      return storeAsBinary;
   }
   
   @Override
   public LoadersConfigurationBuilder loaders() {
      return loaders;
   }
   
   @Override
   public LockingConfigurationBuilder locking() {
      return locking;
   }
   
   @Override
   public TransactionConfigurationBuilder transaction() {
      return transaction;
   }

   @Override
   public VersioningConfigurationBuilder versioning() {
      return versioning;
   }
   
   @Override
   public UnsafeConfigurationBuilder unsafe() {
      return unsafe;
   }

   @SuppressWarnings("unchecked")
   public void validate() {
      for (AbstractConfigurationChildBuilder<?> validatable:
            asList(clustering, dataContainer, deadlockDetection, eviction, expiration, indexing,
                   invocationBatching, jmxStatistics, loaders, locking, storeAsBinary, transaction,
                   versioning, unsafe)) {
         validatable.validate();
      }

      // TODO validate that a transport is set if a singleton store is set
   }

   @Override
   public Configuration build() {
      validate();
      return new Configuration(clustering.create(),
            customInterceptors.create(),
            dataContainer.create(),
            deadlockDetection.create(),
            eviction.create(),
            expiration.create(),
            indexing.create(),
            invocationBatching.create(),
            jmxStatistics.create(),
            loaders.create(),
            locking.create(),
            storeAsBinary.create(),
            transaction.create(),
            unsafe.create(),
            versioning.create(),
            classLoader );// TODO
   }

   public ConfigurationBuilder read(Configuration template) {
      this.classLoader = template.classLoader();
      this.clustering.read(template.clustering());
      this.customInterceptors.read(template.customInterceptors());
      this.dataContainer.read(template.dataContainer());
      this.deadlockDetection.read(template.deadlockDetection());
      this.eviction.read(template.eviction());
      this.expiration.read(template.expiration());
      this.indexing.read(template.indexing());
      this.invocationBatching.read(template.invocationBatching());
      this.jmxStatistics.read(template.jmxStatistics());
      this.loaders.read(template.loaders());
      this.locking.read(template.locking());
      this.storeAsBinary.read(template.storeAsBinary());
      this.transaction.read(template.transaction());
      this.unsafe.read(template.unsafe());
      this.versioning.read(template.versioning());
      
      return this;
   }

   @Override
   public String toString() {
      return "ConfigurationBuilder{" +
            "classLoader=" + classLoader +
            ", clustering=" + clustering +
            ", customInterceptors=" + customInterceptors +
            ", dataContainer=" + dataContainer +
            ", deadlockDetection=" + deadlockDetection +
            ", eviction=" + eviction +
            ", expiration=" + expiration +
            ", indexing=" + indexing +
            ", invocationBatching=" + invocationBatching +
            ", jmxStatistics=" + jmxStatistics +
            ", loaders=" + loaders +
            ", locking=" + locking +
            ", storeAsBinary=" + storeAsBinary +
            ", transaction=" + transaction +
            ", versioning=" + versioning +
            ", unsafe=" + unsafe +
            '}';
   }

}

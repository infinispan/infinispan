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

public class Configuration {
   
   private final ClassLoader classLoader; //TODO remove this
   private final ClusteringConfiguration clusteringConfiguration;
   private final CustomInterceptorsConfiguration customInterceptorsConfiguration;
   private final DataContainerConfiguration dataContainerConfiguration;
   private final DeadlockDetectionConfiguration deadlockDetectionConfiguration;
   private final EvictionConfiguration evictionConfiguration;
   private final ExpirationConfiguration expirationConfiguration;
   private final IndexingConfiguration indexingConfiguration;
   private final InvocationBatchingConfiguration invocationBatchingConfiguration;
   private final JMXStatisticsConfiguration jmxStatisticsConfiguration;
   private final LoadersConfiguration loadersConfiguration;
   private final LockingConfiguration lockingConfiguration;
   private final StoreAsBinaryConfiguration storeAsBinaryConfiguration;
   private final TransactionConfiguration transactionConfiguration;
   private final VersioningConfiguration versioningConfiguration;
   private final UnsafeConfiguration unsafeConfiguration;

   Configuration(ClusteringConfiguration clusteringConfiguration,
         CustomInterceptorsConfiguration customInterceptorsConfiguration,
         DataContainerConfiguration dataContainerConfiguration, DeadlockDetectionConfiguration deadlockDetectionConfiguration,
         EvictionConfiguration evictionConfiguration, ExpirationConfiguration expirationConfiguration,
         IndexingConfiguration indexingConfiguration, InvocationBatchingConfiguration invocationBatchingConfiguration,
         JMXStatisticsConfiguration jmxStatisticsConfiguration,
         LoadersConfiguration loadersConfiguration,
         LockingConfiguration lockingConfiguration, StoreAsBinaryConfiguration storeAsBinaryConfiguration,
         TransactionConfiguration transactionConfiguration, UnsafeConfiguration unsafeConfiguration,
         VersioningConfiguration versioningConfiguration, ClassLoader cl) {
      this.clusteringConfiguration = clusteringConfiguration;
      this.customInterceptorsConfiguration = customInterceptorsConfiguration;
      this.dataContainerConfiguration = dataContainerConfiguration;
      this.deadlockDetectionConfiguration = deadlockDetectionConfiguration;
      this.evictionConfiguration = evictionConfiguration;
      this.expirationConfiguration = expirationConfiguration;
      this.indexingConfiguration = indexingConfiguration;
      this.invocationBatchingConfiguration = invocationBatchingConfiguration;
      this.jmxStatisticsConfiguration = jmxStatisticsConfiguration;
      this.loadersConfiguration = loadersConfiguration;
      this.lockingConfiguration = lockingConfiguration;
      this.storeAsBinaryConfiguration = storeAsBinaryConfiguration;
      this.transactionConfiguration = transactionConfiguration;
      this.unsafeConfiguration = unsafeConfiguration;
      this.versioningConfiguration = versioningConfiguration;
      this.classLoader = cl;
   }
   
   /**
    * Will be removed with no replacement
    * @return
    */
   @Deprecated
   public ClassLoader classLoader() {
      return classLoader;
   }
   
   public ClusteringConfiguration clustering() {
      return clusteringConfiguration;
   }
   
   public CustomInterceptorsConfiguration customInterceptors() {
      return customInterceptorsConfiguration;
   }
   
   public DataContainerConfiguration dataContainer() {
      return dataContainerConfiguration;
   }
   
   public DeadlockDetectionConfiguration deadlockDetection() {
      return deadlockDetectionConfiguration;
   }
   
   public EvictionConfiguration eviction() {
      return evictionConfiguration;
   }
   
   public ExpirationConfiguration expiration() {
      return expirationConfiguration;
   }
   
   public IndexingConfiguration indexing() {
      return indexingConfiguration;
   }
   
   public InvocationBatchingConfiguration invocationBatching() {
      return invocationBatchingConfiguration;
   }
   
   public JMXStatisticsConfiguration jmxStatistics() {
      return jmxStatisticsConfiguration;
   }
   
   public LoadersConfiguration loaders() {
      return loadersConfiguration;
   }
   
   public LockingConfiguration locking() {
      return lockingConfiguration;
   }
   
   public StoreAsBinaryConfiguration storeAsBinary() {
      return storeAsBinaryConfiguration;
   }
   
   public TransactionConfiguration transaction() {
      return transactionConfiguration;
   }
   
   public UnsafeConfiguration unsafe() {
      return unsafeConfiguration;
   }

   public VersioningConfiguration versioning() {
      return versioningConfiguration;
   }

   @Override
   public String toString() {
      return "Configuration{" +
            "classLoader=" + classLoader +
            ", clustering=" + clusteringConfiguration +
            ", customInterceptors=" + customInterceptorsConfiguration +
            ", dataContainer=" + dataContainerConfiguration +
            ", deadlockDetection=" + deadlockDetectionConfiguration +
            ", eviction=" + evictionConfiguration +
            ", expiration=" + expirationConfiguration +
            ", indexing=" + indexingConfiguration +
            ", invocationBatching=" + invocationBatchingConfiguration +
            ", jmxStatistics=" + jmxStatisticsConfiguration +
            ", loaders=" + loadersConfiguration +
            ", locking=" + lockingConfiguration +
            ", storeAsBinary=" + storeAsBinaryConfiguration +
            ", transaction=" + transactionConfiguration +
            ", versioning=" + versioningConfiguration +
            ", unsafe=" + unsafeConfiguration +
            '}';
   }

}

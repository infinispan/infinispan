package org.jboss.as.clustering.infinispan.cs.factory;


import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Self;
import org.infinispan.configuration.cache.AsyncStoreConfigurationBuilder;
import org.infinispan.configuration.cache.ClusteringConfigurationBuilder;
import org.infinispan.configuration.cache.CompatibilityModeConfigurationBuilder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationChildBuilder;
import org.infinispan.configuration.cache.CustomInterceptorsConfigurationBuilder;
import org.infinispan.configuration.cache.DataContainerConfigurationBuilder;
import org.infinispan.configuration.cache.DeadlockDetectionConfigurationBuilder;
import org.infinispan.configuration.cache.EvictionConfigurationBuilder;
import org.infinispan.configuration.cache.ExpirationConfigurationBuilder;
import org.infinispan.configuration.cache.IndexingConfigurationBuilder;
import org.infinispan.configuration.cache.InvocationBatchingConfigurationBuilder;
import org.infinispan.configuration.cache.JMXStatisticsConfigurationBuilder;
import org.infinispan.configuration.cache.LockingConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SecurityConfigurationBuilder;
import org.infinispan.configuration.cache.SingletonStoreConfigurationBuilder;
import org.infinispan.configuration.cache.SitesConfigurationBuilder;
import org.infinispan.configuration.cache.StoreAsBinaryConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.cache.TransactionConfigurationBuilder;
import org.infinispan.configuration.cache.UnsafeConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;

import java.util.Properties;

public class CustomStoreConfigurationBuilder implements StoreConfigurationBuilder {

   @Override
   public void validate() {
   }

   @Override
   public Object create() {
      return null;
   }

   @Override
   public Builder<?> read(Object template) {
      return null;
   }

   @Override
   public ConfigurationChildBuilder simpleCache(boolean simpleCache) {
      return null;
   }

   @Override
   public boolean simpleCache() {
      return false;
   }

   @Override
   public ConfigurationChildBuilder inlineInterceptors(boolean inlineInterceptors) {
      return null;
   }

   @Override
   public boolean inlineInterceptors() {
      return false;
   }

   @Override
   public AsyncStoreConfigurationBuilder async() {
      return null;
   }

   @Override
   public SingletonStoreConfigurationBuilder singleton() {
      return null;
   }

   @Override
   public Object fetchPersistentState(boolean b) {
      return null;
   }

   @Override
   public Object ignoreModifications(boolean b) {
      return null;
   }

   @Override
   public Object purgeOnStartup(boolean b) {
      return null;
   }

   @Override
   public Object preload(boolean b) {
      return null;
   }

   @Override
   public Object shared(boolean b) {
      return null;
   }

   @Override
   public Object addProperty(String key, String value) {
      return null;
   }

   @Override
   public Object withProperties(Properties p) {
      return null;
   }

   @Override
   public ClusteringConfigurationBuilder clustering() {
      return null;
   }

   @Override
   public CustomInterceptorsConfigurationBuilder customInterceptors() {
      return null;
   }

   @Override
   public DataContainerConfigurationBuilder dataContainer() {
      return null;
   }

   @Override
   public DeadlockDetectionConfigurationBuilder deadlockDetection() {
      return null;
   }

   @Override
   public EvictionConfigurationBuilder eviction() {
      return null;
   }

   @Override
   public ExpirationConfigurationBuilder expiration() {
      return null;
   }

   @Override
   public IndexingConfigurationBuilder indexing() {
      return null;
   }

   @Override
   public InvocationBatchingConfigurationBuilder invocationBatching() {
      return null;
   }

   @Override
   public JMXStatisticsConfigurationBuilder jmxStatistics() {
      return null;
   }

   @Override
   public PersistenceConfigurationBuilder persistence() {
      return null;
   }

   @Override
   public LockingConfigurationBuilder locking() {
      return null;
   }

   @Override
   public SecurityConfigurationBuilder security() {
      return null;
   }

   @Override
   public StoreAsBinaryConfigurationBuilder storeAsBinary() {
      return null;
   }

   @Override
   public TransactionConfigurationBuilder transaction() {
      return null;
   }

   @Override
   public VersioningConfigurationBuilder versioning() {
      return null;
   }

   @Override
   public UnsafeConfigurationBuilder unsafe() {
      return null;
   }

   @Override
   public SitesConfigurationBuilder sites() {
      return null;
   }

   @Override
   public CompatibilityModeConfigurationBuilder compatibility() {
      return null;
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public Configuration build() {
      return null;
   }

   @Override
   public Self self() {
      return null;
   }
}

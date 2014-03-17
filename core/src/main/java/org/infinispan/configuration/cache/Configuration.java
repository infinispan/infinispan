package org.infinispan.configuration.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Configuration {

   private final ClusteringConfiguration clusteringConfiguration;
   private final CustomInterceptorsConfiguration customInterceptorsConfiguration;
   private final DataContainerConfiguration dataContainerConfiguration;
   private final DeadlockDetectionConfiguration deadlockDetectionConfiguration;
   private final EvictionConfiguration evictionConfiguration;
   private final ExpirationConfiguration expirationConfiguration;
   private final IndexingConfiguration indexingConfiguration;
   private final InvocationBatchingConfiguration invocationBatchingConfiguration;
   private final JMXStatisticsConfiguration jmxStatisticsConfiguration;
   private final PersistenceConfiguration persistenceConfiguration;
   private final LockingConfiguration lockingConfiguration;
   private final StoreAsBinaryConfiguration storeAsBinaryConfiguration;
   private final TransactionConfiguration transactionConfiguration;
   private final VersioningConfiguration versioningConfiguration;
   private final UnsafeConfiguration unsafeConfiguration;
   private final Map<Class<?>, ?> moduleConfiguration;
   private final SitesConfiguration sitesConfiguration;
   private final CompatibilityModeConfiguration compatibilityConfiguration;

   Configuration(ClusteringConfiguration clusteringConfiguration,
                 CustomInterceptorsConfiguration customInterceptorsConfiguration,
                 DataContainerConfiguration dataContainerConfiguration, DeadlockDetectionConfiguration deadlockDetectionConfiguration,
                 EvictionConfiguration evictionConfiguration, ExpirationConfiguration expirationConfiguration,
                 IndexingConfiguration indexingConfiguration, InvocationBatchingConfiguration invocationBatchingConfiguration,
                 JMXStatisticsConfiguration jmxStatisticsConfiguration,
                 PersistenceConfiguration persistenceConfiguration,
                 LockingConfiguration lockingConfiguration, StoreAsBinaryConfiguration storeAsBinaryConfiguration,
                 TransactionConfiguration transactionConfiguration, UnsafeConfiguration unsafeConfiguration,
                 VersioningConfiguration versioningConfiguration, SitesConfiguration sitesConfiguration,
                 CompatibilityModeConfiguration compatibilityConfiguration,
                 List<?> modules) {
      this.clusteringConfiguration = clusteringConfiguration;
      this.customInterceptorsConfiguration = customInterceptorsConfiguration;
      this.dataContainerConfiguration = dataContainerConfiguration;
      this.deadlockDetectionConfiguration = deadlockDetectionConfiguration;
      this.evictionConfiguration = evictionConfiguration;
      this.expirationConfiguration = expirationConfiguration;
      this.indexingConfiguration = indexingConfiguration;
      this.invocationBatchingConfiguration = invocationBatchingConfiguration;
      this.jmxStatisticsConfiguration = jmxStatisticsConfiguration;
      this.persistenceConfiguration = persistenceConfiguration;
      this.lockingConfiguration = lockingConfiguration;
      this.storeAsBinaryConfiguration = storeAsBinaryConfiguration;
      this.transactionConfiguration = transactionConfiguration;
      this.unsafeConfiguration = unsafeConfiguration;
      this.versioningConfiguration = versioningConfiguration;
      this.sitesConfiguration = sitesConfiguration;
      this.compatibilityConfiguration = compatibilityConfiguration;
      Map<Class<?>, Object> modulesMap = new HashMap<Class<?>, Object>();
      for(Object module : modules) {
         modulesMap.put(module.getClass(), module);
      }
      this.moduleConfiguration = Collections.unmodifiableMap(modulesMap);
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

   public PersistenceConfiguration persistence() {
      return persistenceConfiguration;
   }

   public LockingConfiguration locking() {
      return lockingConfiguration;
   }

   @SuppressWarnings("unchecked")
   public <T> T module(Class<T> moduleClass) {
      return (T)moduleConfiguration.get(moduleClass);
   }

   public Map<Class<?>, ?> modules() {
      return moduleConfiguration;
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

   public SitesConfiguration sites() {
      return sitesConfiguration;
   }

   public VersioningConfiguration versioning() {
      return versioningConfiguration;
   }

   public CompatibilityModeConfiguration compatibility() {
      return compatibilityConfiguration;
   }

   @Override
   public String toString() {
      return "Configuration{" +
            "clustering=" + clusteringConfiguration +
            ", customInterceptors=" + customInterceptorsConfiguration +
            ", dataContainer=" + dataContainerConfiguration +
            ", deadlockDetection=" + deadlockDetectionConfiguration +
            ", eviction=" + evictionConfiguration +
            ", expiration=" + expirationConfiguration +
            ", indexing=" + indexingConfiguration +
            ", invocationBatching=" + invocationBatchingConfiguration +
            ", jmxStatistics=" + jmxStatisticsConfiguration +
            ", persistence=" + persistenceConfiguration +
            ", locking=" + lockingConfiguration +
            ", modules=" + moduleConfiguration +
            ", storeAsBinary=" + storeAsBinaryConfiguration +
            ", transaction=" + transactionConfiguration +
            ", versioning=" + versioningConfiguration +
            ", unsafe=" + unsafeConfiguration +
            ", sites=" + sitesConfiguration +
            ", compatibility=" + compatibilityConfiguration +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Configuration that = (Configuration) o;

      if (clusteringConfiguration != null ? !clusteringConfiguration.equals(that.clusteringConfiguration) : that.clusteringConfiguration != null)
         return false;
      if (customInterceptorsConfiguration != null ? !customInterceptorsConfiguration.equals(that.customInterceptorsConfiguration) : that.customInterceptorsConfiguration != null)
         return false;
      if (dataContainerConfiguration != null ? !dataContainerConfiguration.equals(that.dataContainerConfiguration) : that.dataContainerConfiguration != null)
         return false;
      if (deadlockDetectionConfiguration != null ? !deadlockDetectionConfiguration.equals(that.deadlockDetectionConfiguration) : that.deadlockDetectionConfiguration != null)
         return false;
      if (evictionConfiguration != null ? !evictionConfiguration.equals(that.evictionConfiguration) : that.evictionConfiguration != null)
         return false;
      if (expirationConfiguration != null ? !expirationConfiguration.equals(that.expirationConfiguration) : that.expirationConfiguration != null)
         return false;
      if (indexingConfiguration != null ? !indexingConfiguration.equals(that.indexingConfiguration) : that.indexingConfiguration != null)
         return false;
      if (invocationBatchingConfiguration != null ? !invocationBatchingConfiguration.equals(that.invocationBatchingConfiguration) : that.invocationBatchingConfiguration != null)
         return false;
      if (jmxStatisticsConfiguration != null ? !jmxStatisticsConfiguration.equals(that.jmxStatisticsConfiguration) : that.jmxStatisticsConfiguration != null)
         return false;
      if (persistenceConfiguration != null ? !persistenceConfiguration.equals(that.persistenceConfiguration) : that.persistenceConfiguration != null)
         return false;
      if (lockingConfiguration != null ? !lockingConfiguration.equals(that.lockingConfiguration) : that.lockingConfiguration != null)
         return false;
      if (moduleConfiguration != null ? !moduleConfiguration.equals(that.moduleConfiguration) : that.moduleConfiguration !=null)
         return false;
      if (storeAsBinaryConfiguration != null ? !storeAsBinaryConfiguration.equals(that.storeAsBinaryConfiguration) : that.storeAsBinaryConfiguration != null)
         return false;
      if (transactionConfiguration != null ? !transactionConfiguration.equals(that.transactionConfiguration) : that.transactionConfiguration != null)
         return false;
      if (unsafeConfiguration != null ? !unsafeConfiguration.equals(that.unsafeConfiguration) : that.unsafeConfiguration != null)
         return false;
      if (sitesConfiguration != null ? !sitesConfiguration.equals(that.sitesConfiguration) : that.sitesConfiguration != null)
         return false;
      if (versioningConfiguration != null ? !versioningConfiguration.equals(that.versioningConfiguration) : that.versioningConfiguration != null)
         return false;
      if (compatibilityConfiguration != null ? !compatibilityConfiguration.equals(that.compatibilityConfiguration) : that.compatibilityConfiguration != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
	  int result = (clusteringConfiguration != null ? clusteringConfiguration.hashCode() : 0);
      result = 31 * result + (customInterceptorsConfiguration != null ? customInterceptorsConfiguration.hashCode() : 0);
      result = 31 * result + (dataContainerConfiguration != null ? dataContainerConfiguration.hashCode() : 0);
      result = 31 * result + (deadlockDetectionConfiguration != null ? deadlockDetectionConfiguration.hashCode() : 0);
      result = 31 * result + (evictionConfiguration != null ? evictionConfiguration.hashCode() : 0);
      result = 31 * result + (expirationConfiguration != null ? expirationConfiguration.hashCode() : 0);
      result = 31 * result + (indexingConfiguration != null ? indexingConfiguration.hashCode() : 0);
      result = 31 * result + (invocationBatchingConfiguration != null ? invocationBatchingConfiguration.hashCode() : 0);
      result = 31 * result + (jmxStatisticsConfiguration != null ? jmxStatisticsConfiguration.hashCode() : 0);
      result = 31 * result + (persistenceConfiguration != null ? persistenceConfiguration.hashCode() : 0);
      result = 31 * result + (lockingConfiguration != null ? lockingConfiguration.hashCode() : 0);
      result = 31 * result + (moduleConfiguration != null ? moduleConfiguration.hashCode() : 0);
      result = 31 * result + (storeAsBinaryConfiguration != null ? storeAsBinaryConfiguration.hashCode() : 0);
      result = 31 * result + (transactionConfiguration != null ? transactionConfiguration.hashCode() : 0);
      result = 31 * result + (versioningConfiguration != null ? versioningConfiguration.hashCode() : 0);
      result = 31 * result + (unsafeConfiguration != null ? unsafeConfiguration.hashCode() : 0);
      result = 31 * result + (sitesConfiguration != null ? sitesConfiguration.hashCode() : 0);
      result = 31 * result + (compatibilityConfiguration != null ? compatibilityConfiguration.hashCode() : 0);
      return result;
   }
}

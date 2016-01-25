package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Configuration {
   public static final AttributeDefinition<Boolean> SIMPLE_CACHE = AttributeDefinition.builder("simpleCache", false).immutable().build();
   public static final AttributeDefinition<Boolean> INLINE_INTERCEPTORS = AttributeDefinition.builder("inlineInterceptors", false).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(Configuration.class, SIMPLE_CACHE, INLINE_INTERCEPTORS);
   }

   private final Attribute<Boolean> simpleCache;
   private final Attribute<Boolean> inlineInterceptors;
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
   private final SecurityConfiguration securityConfiguration;
   private final SitesConfiguration sitesConfiguration;
   private final CompatibilityModeConfiguration compatibilityConfiguration;
   private final AttributeSet attributes;
   private final boolean template;

   Configuration(boolean template, AttributeSet attributes,
                 ClusteringConfiguration clusteringConfiguration,
                 CustomInterceptorsConfiguration customInterceptorsConfiguration,
                 DataContainerConfiguration dataContainerConfiguration, DeadlockDetectionConfiguration deadlockDetectionConfiguration,
                 EvictionConfiguration evictionConfiguration, ExpirationConfiguration expirationConfiguration,
                 IndexingConfiguration indexingConfiguration, InvocationBatchingConfiguration invocationBatchingConfiguration,
                 JMXStatisticsConfiguration jmxStatisticsConfiguration,
                 PersistenceConfiguration persistenceConfiguration,
                 LockingConfiguration lockingConfiguration,
                 SecurityConfiguration securityConfiguration,
                 StoreAsBinaryConfiguration storeAsBinaryConfiguration,
                 TransactionConfiguration transactionConfiguration, UnsafeConfiguration unsafeConfiguration,
                 VersioningConfiguration versioningConfiguration,
                 SitesConfiguration sitesConfiguration,
                 CompatibilityModeConfiguration compatibilityConfiguration,
                 List<?> modules) {
      this.template = template;
      this.attributes = attributes.checkProtection();
      this.simpleCache = attributes.attribute(SIMPLE_CACHE);
      this.inlineInterceptors = attributes.attribute(INLINE_INTERCEPTORS);
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
      this.securityConfiguration = securityConfiguration;
      this.sitesConfiguration = sitesConfiguration;
      this.compatibilityConfiguration = compatibilityConfiguration;
      Map<Class<?>, Object> modulesMap = new HashMap<Class<?>, Object>();
      for(Object module : modules) {
         modulesMap.put(module.getClass(), module);
      }
      this.moduleConfiguration = Collections.unmodifiableMap(modulesMap);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public boolean simpleCache() {
      return simpleCache.get();
   }

   public boolean inlineInterceptors() {
      return inlineInterceptors.get();
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

   public SecurityConfiguration security() {
      return securityConfiguration;
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

   public boolean isTemplate() {
      return template;
   }

   @Override
   public String toString() {
      return "Configuration{" +
            "simpleCache=" + simpleCache +
            ", inlineInterceptors=" + inlineInterceptors +
            ", clustering=" + clusteringConfiguration +
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
            ", security=" + securityConfiguration +
            ", storeAsBinary=" + storeAsBinaryConfiguration +
            ", transaction=" + transactionConfiguration +
            ", versioning=" + versioningConfiguration +
            ", unsafe=" + unsafeConfiguration +
            ", sites=" + sitesConfiguration +
            ", compatibility=" + compatibilityConfiguration +
            '}';
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (simpleCache.get() ? 0 : 1);
      result = prime * result + (inlineInterceptors.get() ? 0 : 1);
      result = prime * result + (template ? 1231 : 1237);
      result = prime * result + ((clusteringConfiguration == null) ? 0 : clusteringConfiguration.hashCode());
      result = prime * result + ((compatibilityConfiguration == null) ? 0 : compatibilityConfiguration.hashCode());
      result = prime * result
            + ((customInterceptorsConfiguration == null) ? 0 : customInterceptorsConfiguration.hashCode());
      result = prime * result + ((dataContainerConfiguration == null) ? 0 : dataContainerConfiguration.hashCode());
      result = prime * result
            + ((deadlockDetectionConfiguration == null) ? 0 : deadlockDetectionConfiguration.hashCode());
      result = prime * result + ((evictionConfiguration == null) ? 0 : evictionConfiguration.hashCode());
      result = prime * result + ((expirationConfiguration == null) ? 0 : expirationConfiguration.hashCode());
      result = prime * result + ((indexingConfiguration == null) ? 0 : indexingConfiguration.hashCode());
      result = prime * result
            + ((invocationBatchingConfiguration == null) ? 0 : invocationBatchingConfiguration.hashCode());
      result = prime * result + ((jmxStatisticsConfiguration == null) ? 0 : jmxStatisticsConfiguration.hashCode());
      result = prime * result + ((lockingConfiguration == null) ? 0 : lockingConfiguration.hashCode());
      result = prime * result + ((moduleConfiguration == null) ? 0 : moduleConfiguration.hashCode());
      result = prime * result + ((persistenceConfiguration == null) ? 0 : persistenceConfiguration.hashCode());
      result = prime * result + ((securityConfiguration == null) ? 0 : securityConfiguration.hashCode());
      result = prime * result + ((sitesConfiguration == null) ? 0 : sitesConfiguration.hashCode());
      result = prime * result + ((storeAsBinaryConfiguration == null) ? 0 : storeAsBinaryConfiguration.hashCode());
      result = prime * result + ((transactionConfiguration == null) ? 0 : transactionConfiguration.hashCode());
      result = prime * result + ((unsafeConfiguration == null) ? 0 : unsafeConfiguration.hashCode());
      result = prime * result + ((versioningConfiguration == null) ? 0 : versioningConfiguration.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      Configuration other = (Configuration) obj;
      if (template != other.template) {
         return false;
      }
      if (!simpleCache.get().equals(other.simpleCache.get())) {
         return false;
      }
      if (!inlineInterceptors.get().equals(other.inlineInterceptors.get())) {
         return false;
      }
      if (clusteringConfiguration == null) {
         if (other.clusteringConfiguration != null)
            return false;
      } else if (!clusteringConfiguration.equals(other.clusteringConfiguration))
         return false;
      if (compatibilityConfiguration == null) {
         if (other.compatibilityConfiguration != null)
            return false;
      } else if (!compatibilityConfiguration.equals(other.compatibilityConfiguration))
         return false;
      if (customInterceptorsConfiguration == null) {
         if (other.customInterceptorsConfiguration != null)
            return false;
      } else if (!customInterceptorsConfiguration.equals(other.customInterceptorsConfiguration))
         return false;
      if (dataContainerConfiguration == null) {
         if (other.dataContainerConfiguration != null)
            return false;
      } else if (!dataContainerConfiguration.equals(other.dataContainerConfiguration))
         return false;
      if (deadlockDetectionConfiguration == null) {
         if (other.deadlockDetectionConfiguration != null)
            return false;
      } else if (!deadlockDetectionConfiguration.equals(other.deadlockDetectionConfiguration))
         return false;
      if (evictionConfiguration == null) {
         if (other.evictionConfiguration != null)
            return false;
      } else if (!evictionConfiguration.equals(other.evictionConfiguration))
         return false;
      if (expirationConfiguration == null) {
         if (other.expirationConfiguration != null)
            return false;
      } else if (!expirationConfiguration.equals(other.expirationConfiguration))
         return false;
      if (indexingConfiguration == null) {
         if (other.indexingConfiguration != null)
            return false;
      } else if (!indexingConfiguration.equals(other.indexingConfiguration))
         return false;
      if (invocationBatchingConfiguration == null) {
         if (other.invocationBatchingConfiguration != null)
            return false;
      } else if (!invocationBatchingConfiguration.equals(other.invocationBatchingConfiguration))
         return false;
      if (jmxStatisticsConfiguration == null) {
         if (other.jmxStatisticsConfiguration != null)
            return false;
      } else if (!jmxStatisticsConfiguration.equals(other.jmxStatisticsConfiguration))
         return false;
      if (lockingConfiguration == null) {
         if (other.lockingConfiguration != null)
            return false;
      } else if (!lockingConfiguration.equals(other.lockingConfiguration))
         return false;
      if (moduleConfiguration == null) {
         if (other.moduleConfiguration != null)
            return false;
      } else if (!moduleConfiguration.equals(other.moduleConfiguration))
         return false;
      if (persistenceConfiguration == null) {
         if (other.persistenceConfiguration != null)
            return false;
      } else if (!persistenceConfiguration.equals(other.persistenceConfiguration))
         return false;
      if (securityConfiguration == null) {
         if (other.securityConfiguration != null)
            return false;
      } else if (!securityConfiguration.equals(other.securityConfiguration))
         return false;
      if (sitesConfiguration == null) {
         if (other.sitesConfiguration != null)
            return false;
      } else if (!sitesConfiguration.equals(other.sitesConfiguration))
         return false;
      if (storeAsBinaryConfiguration == null) {
         if (other.storeAsBinaryConfiguration != null)
            return false;
      } else if (!storeAsBinaryConfiguration.equals(other.storeAsBinaryConfiguration))
         return false;
      if (transactionConfiguration == null) {
         if (other.transactionConfiguration != null)
            return false;
      } else if (!transactionConfiguration.equals(other.transactionConfiguration))
         return false;
      if (unsafeConfiguration == null) {
         if (other.unsafeConfiguration != null)
            return false;
      } else if (!unsafeConfiguration.equals(other.unsafeConfiguration))
         return false;
      if (versioningConfiguration == null) {
         if (other.versioningConfiguration != null)
            return false;
      } else if (!versioningConfiguration.equals(other.versioningConfiguration))
         return false;
      return true;
   }
}

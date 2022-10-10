package org.infinispan.configuration.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.configuration.BasicConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.configuration.parsing.ParserRegistry;

public class Configuration extends ConfigurationElement<Configuration> implements BasicConfiguration {

   public static final AttributeDefinition<Boolean> SIMPLE_CACHE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SIMPLE_CACHE, false).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(Configuration.class, SIMPLE_CACHE);
   }

   private final Attribute<Boolean> simpleCache;
   private final ClusteringConfiguration clusteringConfiguration;
   private final CustomInterceptorsConfiguration customInterceptorsConfiguration;
   private final MemoryConfiguration memoryConfiguration;
   private final EncodingConfiguration encodingConfiguration;
   private final ExpirationConfiguration expirationConfiguration;
   private final QueryConfiguration queryConfiguration;
   private final IndexingConfiguration indexingConfiguration;
   private final InvocationBatchingConfiguration invocationBatchingConfiguration;
   private final StatisticsConfiguration statisticsConfiguration;
   private final PersistenceConfiguration persistenceConfiguration;
   private final LockingConfiguration lockingConfiguration;
   private final TransactionConfiguration transactionConfiguration;
   private final UnsafeConfiguration unsafeConfiguration;
   private final Map<Class<?>, ?> moduleConfiguration;
   private final SecurityConfiguration securityConfiguration;
   private final SitesConfiguration sitesConfiguration;
   private final boolean template;

   Configuration(boolean template, AttributeSet attributes,
                 ClusteringConfiguration clusteringConfiguration,
                 CustomInterceptorsConfiguration customInterceptorsConfiguration,
                 ExpirationConfiguration expirationConfiguration,
                 EncodingConfiguration encodingConfiguration,
                 QueryConfiguration queryConfiguration,
                 IndexingConfiguration indexingConfiguration,
                 InvocationBatchingConfiguration invocationBatchingConfiguration,
                 StatisticsConfiguration statisticsConfiguration,
                 PersistenceConfiguration persistenceConfiguration,
                 LockingConfiguration lockingConfiguration,
                 SecurityConfiguration securityConfiguration,
                 TransactionConfiguration transactionConfiguration,
                 UnsafeConfiguration unsafeConfiguration,
                 SitesConfiguration sitesConfiguration,
                 MemoryConfiguration memoryConfiguration,
                 List<?> modules) {
      super(clusteringConfiguration.cacheMode().toElement(template), attributes,
            clusteringConfiguration,
            expirationConfiguration,
            encodingConfiguration,
            statisticsConfiguration,
            lockingConfiguration,
            transactionConfiguration,
            unsafeConfiguration,
            sitesConfiguration,
            memoryConfiguration);
      this.template = template;
      this.simpleCache = attributes.attribute(SIMPLE_CACHE);
      this.clusteringConfiguration = clusteringConfiguration;
      this.customInterceptorsConfiguration = customInterceptorsConfiguration;
      this.encodingConfiguration = encodingConfiguration;
      this.expirationConfiguration = expirationConfiguration;
      this.queryConfiguration = queryConfiguration;
      this.indexingConfiguration = indexingConfiguration;
      this.invocationBatchingConfiguration = invocationBatchingConfiguration;
      this.statisticsConfiguration = statisticsConfiguration;
      this.persistenceConfiguration = persistenceConfiguration;
      this.lockingConfiguration = lockingConfiguration;
      this.transactionConfiguration = transactionConfiguration;
      this.unsafeConfiguration = unsafeConfiguration;
      this.securityConfiguration = securityConfiguration;
      this.sitesConfiguration = sitesConfiguration;
      this.memoryConfiguration = memoryConfiguration;
      Map<Class<?>, Object> modulesMap = new HashMap<>();
      for(Object module : modules) {
         modulesMap.put(module.getClass(), module);
      }
      this.moduleConfiguration = Collections.unmodifiableMap(modulesMap);
   }

   public boolean simpleCache() {
      return simpleCache.get();
   }

   public ClusteringConfiguration clustering() {
      return clusteringConfiguration;
   }

   /**
    * @deprecated Since 10.0, custom interceptors support will be removed and only modules will be able to define interceptors
    */
   @Deprecated
   public CustomInterceptorsConfiguration customInterceptors() {
      return customInterceptorsConfiguration;
   }

   public EncodingConfiguration encoding() {
      return encodingConfiguration;
   }

   public ExpirationConfiguration expiration() {
      return expirationConfiguration;
   }

   public QueryConfiguration query() {
      return queryConfiguration;
   }

   public IndexingConfiguration indexing() {
      return indexingConfiguration;
   }

   public InvocationBatchingConfiguration invocationBatching() {
      return invocationBatchingConfiguration;
   }

   public StatisticsConfiguration statistics() {
      return statisticsConfiguration;
   }

   /**
    * @deprecated since 10.1.3 use {@link #statistics} instead. This will be removed in next major version.
    */
   @Deprecated
   public JMXStatisticsConfiguration jmxStatistics() {
      return statistics();
   }

   public PersistenceConfiguration persistence() {
      return persistenceConfiguration;
   }

   public LockingConfiguration locking() {
      return lockingConfiguration;
   }

   public MemoryConfiguration memory() { return memoryConfiguration; }

   @SuppressWarnings("unchecked")
   public <T> T module(Class<T> moduleClass) {
      return (T)moduleConfiguration.get(moduleClass);
   }

   public Map<Class<?>, ?> modules() {
      return moduleConfiguration;
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

   public boolean isTemplate() {
      return template;
   }

   @Override
   public String toString() {
      return "Configuration{" +
            "simpleCache=" + simpleCache +
            ", clustering=" + clusteringConfiguration +
            ", customInterceptors=" + customInterceptorsConfiguration +
            ", encodingConfiguration= " + encodingConfiguration +
            ", expiration=" + expirationConfiguration +
            ", indexing=" + indexingConfiguration +
            ", invocationBatching=" + invocationBatchingConfiguration +
            ", statistics=" + statisticsConfiguration +
            ", persistence=" + persistenceConfiguration +
            ", locking=" + lockingConfiguration +
            ", modules=" + moduleConfiguration +
            ", security=" + securityConfiguration +
            ", transaction=" + transactionConfiguration +
            ", unsafe=" + unsafeConfiguration +
            ", sites=" + sitesConfiguration +
            ", memory=" + memoryConfiguration +
            '}';
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (simpleCache.get() ? 0 : 1);
      result = prime * result + (template ? 1231 : 1237);
      result = prime * result + ((clusteringConfiguration == null) ? 0 : clusteringConfiguration.hashCode());
      result = prime * result
            + ((customInterceptorsConfiguration == null) ? 0 : customInterceptorsConfiguration.hashCode());
      result = prime * result + ((encodingConfiguration == null) ? 0 : encodingConfiguration.hashCode());
      result = prime * result + ((expirationConfiguration == null) ? 0 : expirationConfiguration.hashCode());
      result = prime * result + ((indexingConfiguration == null) ? 0 : indexingConfiguration.hashCode());
      result = prime * result
            + ((invocationBatchingConfiguration == null) ? 0 : invocationBatchingConfiguration.hashCode());
      result = prime * result + ((statisticsConfiguration == null) ? 0 : statisticsConfiguration.hashCode());
      result = prime * result + ((lockingConfiguration == null) ? 0 : lockingConfiguration.hashCode());
      result = prime * result + ((moduleConfiguration == null) ? 0 : moduleConfiguration.hashCode());
      result = prime * result + ((persistenceConfiguration == null) ? 0 : persistenceConfiguration.hashCode());
      result = prime * result + ((securityConfiguration == null) ? 0 : securityConfiguration.hashCode());
      result = prime * result + ((sitesConfiguration == null) ? 0 : sitesConfiguration.hashCode());
      result = prime * result + ((transactionConfiguration == null) ? 0 : transactionConfiguration.hashCode());
      result = prime * result + ((unsafeConfiguration == null) ? 0 : unsafeConfiguration.hashCode());
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
      if (clusteringConfiguration == null) {
         if (other.clusteringConfiguration != null)
            return false;
      } else if (!clusteringConfiguration.equals(other.clusteringConfiguration))
         return false;
      if (customInterceptorsConfiguration == null) {
         if (other.customInterceptorsConfiguration != null)
            return false;
      } else if (!customInterceptorsConfiguration.equals(other.customInterceptorsConfiguration))
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
      if (statisticsConfiguration == null) {
         if (other.statisticsConfiguration != null)
            return false;
      } else if (!statisticsConfiguration.equals(other.statisticsConfiguration))
         return false;
      if (lockingConfiguration == null) {
         if (other.lockingConfiguration != null)
            return false;
      } else if (!lockingConfiguration.equals(other.lockingConfiguration))
         return false;
      if (memoryConfiguration == null) {
         if (other.memoryConfiguration != null)
            return false;
      } else if (!memoryConfiguration.equals(other.memoryConfiguration))
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
      return true;
   }

   @Override
   public boolean matches(Configuration other) {
      if (!simpleCache.get().equals(other.simpleCache.get()))
         return false;
      if (!clusteringConfiguration.matches(other.clusteringConfiguration))
         return false;
      if (!customInterceptorsConfiguration.matches(other.customInterceptorsConfiguration))
         return false;
      if (!expirationConfiguration.matches(other.expirationConfiguration))
         return false;
      if (!indexingConfiguration.matches(other.indexingConfiguration))
         return false;
      if (!invocationBatchingConfiguration.matches(other.invocationBatchingConfiguration))
         return false;
      if (!statisticsConfiguration.matches(other.statisticsConfiguration))
         return false;
      if (!lockingConfiguration.matches(other.lockingConfiguration))
         return false;
      if (!memoryConfiguration.matches(other.memoryConfiguration))
         return false;
      if (!persistenceConfiguration.matches(other.persistenceConfiguration))
         return false;
      if (!securityConfiguration.matches(other.securityConfiguration))
         return false;
      if (!sitesConfiguration.matches(other.sitesConfiguration))
         return false;
      if (!transactionConfiguration.matches(other.transactionConfiguration))
         return false;
      if (!unsafeConfiguration.matches(other.unsafeConfiguration))
         return false;
      for(Map.Entry<Class<?>, ?> module : moduleConfiguration.entrySet()) {
         if (!other.moduleConfiguration.containsKey(module.getKey()))
            return false;
         Object thisModule = module.getValue();
         Object thatModule = other.moduleConfiguration.get(module.getKey());
         if (thisModule instanceof Matchable && (!((Matchable)thisModule).matches(thatModule)))
            return false;
         if (!thisModule.equals(thatModule))
            return false;
      }
      return attributes.matches(other.attributes);
   }

   @Override
   public String toStringConfiguration(String name) {
      ParserRegistry reg = new ParserRegistry();
      return reg.serialize(name, this);
   }
}

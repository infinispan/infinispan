package org.infinispan.configuration.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.configuration.BasicConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.configuration.parsing.ParserRegistry;

public class Configuration extends ConfigurationElement<Configuration> implements BasicConfiguration {
   public static final AttributeDefinition<String> CONFIGURATION = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CONFIGURATION, null, String.class).immutable().build();
   public static final AttributeDefinition<Boolean> SIMPLE_CACHE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SIMPLE_CACHE, false).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(Configuration.class, CONFIGURATION, SIMPLE_CACHE);
   }

   private final Attribute<Boolean> simpleCache;
   private final ClusteringConfiguration clusteringConfiguration;
   private final CustomInterceptorsConfiguration customInterceptorsConfiguration;
   private final EncodingConfiguration encodingConfiguration;
   private final ExpirationConfiguration expirationConfiguration;
   private final IndexingConfiguration indexingConfiguration;
   private final InvocationBatchingConfiguration invocationBatchingConfiguration;
   private final LockingConfiguration lockingConfiguration;
   private final MemoryConfiguration memoryConfiguration;
   private final Map<Class<?>, ?> moduleConfiguration;
   private final PersistenceConfiguration persistenceConfiguration;
   private final QueryConfiguration queryConfiguration;
   private final SecurityConfiguration securityConfiguration;
   private final SitesConfiguration sitesConfiguration;
   private final StatisticsConfiguration statisticsConfiguration;
   private final TransactionConfiguration transactionConfiguration;
   private final UnsafeConfiguration unsafeConfiguration;
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
            queryConfiguration,
            indexingConfiguration,
            statisticsConfiguration,
            persistenceConfiguration,
            lockingConfiguration,
            securityConfiguration,
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
            "simpleCache=" + simpleCache.get() +
            ", clustering=" + clusteringConfiguration +
            ", customInterceptors=" + customInterceptorsConfiguration +
            ", encoding=" + encodingConfiguration +
            ", expiration=" + expirationConfiguration +
            ", query=" + queryConfiguration +
            ", indexing=" + indexingConfiguration +
            ", invocationBatching=" + invocationBatchingConfiguration +
            ", locking=" + lockingConfiguration +
            ", memory=" + memoryConfiguration +
            ", modules=" + moduleConfiguration +
            ", persistence=" + persistenceConfiguration +
            ", security=" + securityConfiguration +
            ", sites=" + sitesConfiguration +
            ", statistics=" + statisticsConfiguration +
            ", transaction=" + transactionConfiguration +
            ", unsafe=" + unsafeConfiguration +
            ", template=" + template +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      Configuration that = (Configuration) o;
      return template == that.template
            && Objects.equals(simpleCache.get(), that.simpleCache.get())
            && Objects.equals(clusteringConfiguration, that.clusteringConfiguration)
            && Objects.equals(customInterceptorsConfiguration, that.customInterceptorsConfiguration)
            && Objects.equals(encodingConfiguration, that.encodingConfiguration)
            && Objects.equals(expirationConfiguration, that.expirationConfiguration)
            && Objects.equals(indexingConfiguration, that.indexingConfiguration)
            && Objects.equals(invocationBatchingConfiguration, that.invocationBatchingConfiguration)
            && Objects.equals(lockingConfiguration, that.lockingConfiguration)
            && Objects.equals(memoryConfiguration, that.memoryConfiguration)
            && Objects.equals(moduleConfiguration, that.moduleConfiguration)
            && Objects.equals(persistenceConfiguration, that.persistenceConfiguration)
            && Objects.equals(queryConfiguration, that.queryConfiguration)
            && Objects.equals(securityConfiguration, that.securityConfiguration)
            && Objects.equals(sitesConfiguration, that.sitesConfiguration)
            && Objects.equals(statisticsConfiguration, that.statisticsConfiguration)
            && Objects.equals(transactionConfiguration, that.transactionConfiguration)
            && Objects.equals(unsafeConfiguration, that.unsafeConfiguration);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), simpleCache.get(),
            clusteringConfiguration,
            customInterceptorsConfiguration,
            encodingConfiguration,
            expirationConfiguration,
            indexingConfiguration,
            invocationBatchingConfiguration,
            lockingConfiguration,
            memoryConfiguration,
            moduleConfiguration,
            persistenceConfiguration,
            queryConfiguration,
            securityConfiguration,
            sitesConfiguration,
            statisticsConfiguration,
            transactionConfiguration,
            unsafeConfiguration,
            template
      );
   }

   @Override
   public boolean matches(Configuration other) {
      if (!simpleCache.get().equals(other.simpleCache.get()))
         return false;
      if (!clusteringConfiguration.matches(other.clusteringConfiguration))
         return false;
      if (!customInterceptorsConfiguration.matches(other.customInterceptorsConfiguration))
         return false;
      if (!encodingConfiguration.matches(other.encodingConfiguration))
         return false;
      if (!expirationConfiguration.matches(other.expirationConfiguration))
         return false;
      if (!indexingConfiguration.matches(other.indexingConfiguration))
         return false;
      if (!invocationBatchingConfiguration.matches(other.invocationBatchingConfiguration))
         return false;
      if (!lockingConfiguration.matches(other.lockingConfiguration))
         return false;
      if (!memoryConfiguration.matches(other.memoryConfiguration))
         return false;
      if (!persistenceConfiguration.matches(other.persistenceConfiguration))
         return false;
      if (!queryConfiguration.matches(other.queryConfiguration))
         return false;
      if (!securityConfiguration.matches(other.securityConfiguration))
         return false;
      if (!sitesConfiguration.matches(other.sitesConfiguration))
         return false;
      if (!statisticsConfiguration.matches(other.statisticsConfiguration))
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

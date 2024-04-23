package org.infinispan.configuration.cache;

import static org.infinispan.commons.configuration.attributes.AttributeSerializer.STRING_COLLECTION;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.configuration.BasicConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeMatcher;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.StringBuilderWriter;
import org.infinispan.configuration.parsing.ParserRegistry;

public class Configuration extends ConfigurationElement<Configuration> implements BasicConfiguration {
   public static final AttributeDefinition<String> CONFIGURATION = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CONFIGURATION, null, String.class).immutable().build();
   public static final AttributeDefinition<Boolean> SIMPLE_CACHE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SIMPLE_CACHE, false).immutable().build();
   @SuppressWarnings("unchecked")
   public static final AttributeDefinition<List<String>> ALIASES = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ALIASES, null, (Class<List<String>>) (Class<?>) List.class)
         .since(15, 0)
         .initializer(ArrayList::new)
         // Ignore attribute in match comparison
         .matcher(AttributeMatcher.alwaysTrue())
         .serializer(STRING_COLLECTION)
         .build();


   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(Configuration.class, CONFIGURATION, SIMPLE_CACHE, ALIASES);
   }

   private final Attribute<Boolean> simpleCache;
   private final ClusteringConfiguration clusteringConfiguration;
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
   private final TracingConfiguration tracingConfiguration;
   private final TransactionConfiguration transactionConfiguration;
   private final UnsafeConfiguration unsafeConfiguration;
   private final boolean template;

   Configuration(boolean template, AttributeSet attributes,
                 ClusteringConfiguration clusteringConfiguration,
                 EncodingConfiguration encodingConfiguration,
                 ExpirationConfiguration expirationConfiguration,
                 IndexingConfiguration indexingConfiguration,
                 InvocationBatchingConfiguration invocationBatchingConfiguration,
                 LockingConfiguration lockingConfiguration,
                 MemoryConfiguration memoryConfiguration,
                 PersistenceConfiguration persistenceConfiguration,
                 QueryConfiguration queryConfiguration,
                 SecurityConfiguration securityConfiguration,
                 SitesConfiguration sitesConfiguration,
                 StatisticsConfiguration statisticsConfiguration,
                 TracingConfiguration tracingConfiguration,
                 TransactionConfiguration transactionConfiguration,
                 UnsafeConfiguration unsafeConfiguration,
                 List<?> modules) {
      super(clusteringConfiguration.cacheMode().toElement(template), attributes,
            clusteringConfiguration,
            encodingConfiguration,
            expirationConfiguration,
            indexingConfiguration,
            lockingConfiguration,
            memoryConfiguration,
            persistenceConfiguration,
            queryConfiguration,
            securityConfiguration,
            sitesConfiguration,
            statisticsConfiguration,
            tracingConfiguration,
            transactionConfiguration,
            unsafeConfiguration);
      this.template = template;
      this.simpleCache = attributes.attribute(SIMPLE_CACHE);
      this.clusteringConfiguration = clusteringConfiguration;
      this.encodingConfiguration = encodingConfiguration;
      this.expirationConfiguration = expirationConfiguration;
      this.queryConfiguration = queryConfiguration;
      this.indexingConfiguration = indexingConfiguration;
      this.tracingConfiguration = tracingConfiguration;
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
      for (Object module : modules) {
         modulesMap.put(module.getClass(), module);
      }
      this.moduleConfiguration = Map.copyOf(modulesMap);
   }

   public List<String> aliases() {
      return attributes.attribute(ALIASES).get();
   }

   public boolean simpleCache() {
      return simpleCache.get();
   }

   public ClusteringConfiguration clustering() {
      return clusteringConfiguration;
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

   public TracingConfiguration tracing() {
      return tracingConfiguration;
   }

   public InvocationBatchingConfiguration invocationBatching() {
      return invocationBatchingConfiguration;
   }

   public StatisticsConfiguration statistics() {
      return statisticsConfiguration;
   }

   public PersistenceConfiguration persistence() {
      return persistenceConfiguration;
   }

   public LockingConfiguration locking() {
      return lockingConfiguration;
   }

   public MemoryConfiguration memory() {
      return memoryConfiguration;
   }

   @SuppressWarnings("unchecked")
   public <T> T module(Class<T> moduleClass) {
      return (T) moduleConfiguration.get(moduleClass);
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
            attributes.toString(null) +
            ", clustering=" + clusteringConfiguration +
            ", encoding=" + encodingConfiguration +
            ", expiration=" + expirationConfiguration +
            ", query=" + queryConfiguration +
            ", indexing=" + indexingConfiguration +
            ", tracing=" + tracingConfiguration +
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
            && Objects.equals(tracingConfiguration, that.tracingConfiguration)
            && Objects.equals(transactionConfiguration, that.transactionConfiguration)
            && Objects.equals(unsafeConfiguration, that.unsafeConfiguration);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), simpleCache.get(),
            clusteringConfiguration,
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
            tracingConfiguration,
            transactionConfiguration,
            unsafeConfiguration,
            template
      );
   }

   @Override
   public boolean matches(Configuration other) {
      if (!clusteringConfiguration.matches(other.clusteringConfiguration))
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
      if (!tracingConfiguration.matches(other.tracingConfiguration))
         return false;
      if (!transactionConfiguration.matches(other.transactionConfiguration))
         return false;
      if (!unsafeConfiguration.matches(other.unsafeConfiguration))
         return false;
      for (Map.Entry<Class<?>, ?> module : moduleConfiguration.entrySet()) {
         if (!other.moduleConfiguration.containsKey(module.getKey()))
            return false;
         Object thisModule = module.getValue();
         Object thatModule = other.moduleConfiguration.get(module.getKey());
         if (thisModule instanceof Matchable && (!((Matchable) thisModule).matches(thatModule)))
            return false;
         if (!thisModule.equals(thatModule))
            return false;
      }
      return attributes.matches(other.attributes);
   }

   @Override
   public String toStringConfiguration(String name, MediaType mediaType, boolean clearTextSecrets) {
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).withType(mediaType).clearTextSecrets(clearTextSecrets).prettyPrint(false).build()) {
         ParserRegistry reg = new ParserRegistry();
         reg.serialize(writer, name, this);
      }
      return sw.toString();
   }
}

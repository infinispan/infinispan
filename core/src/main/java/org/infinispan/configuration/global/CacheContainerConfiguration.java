package org.infinispan.configuration.global;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Map;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Features;
import org.infinispan.util.ByteString;

/*
 * @since 10.0
 */
class CacheContainerConfiguration {

   private static final String ZERO_CAPACITY_NODE_FEATURE = "zero-capacity-node";

   static final AttributeDefinition<String> DEFAULT_CACHE = AttributeDefinition.builder("defaultCache", null, String.class).immutable()
         .validator(value -> {
            if (value != null && !ByteString.isValid(value)) {
               throw CONFIG.invalidNameSize(value);
            }
         }).build();
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", "DefaultCacheManager").immutable().build();
   static final AttributeDefinition<Boolean> STATISTICS = AttributeDefinition.builder("statistics", false).immutable().build();
   static final AttributeDefinition<Boolean> ZERO_CAPACITY_NODE = AttributeDefinition.builder("zeroCapacityNode", Boolean.FALSE).immutable().build();
   static final AttributeDefinition<String> LISTENER_EXECUTOR = AttributeDefinition.builder("listenerExecutor", "listener-pool", String.class).immutable().build();
   static final AttributeDefinition<String> EXPIRATION_EXECUTOR = AttributeDefinition.builder("expirationExecutor", "expiration-pool", String.class).immutable().build();
   static final AttributeDefinition<String> NON_BLOCKING_EXECUTOR = AttributeDefinition.builder("nonBlockingExecutor", "non-blocking-pool", String.class).immutable().build();
   static final AttributeDefinition<String> BLOCKING_EXECUTOR = AttributeDefinition.builder("blockingExecutor", "blocking-pool", String.class).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CacheContainerConfiguration.class, NAME, STATISTICS, ZERO_CAPACITY_NODE, DEFAULT_CACHE,
            LISTENER_EXECUTOR, EXPIRATION_EXECUTOR,
            NON_BLOCKING_EXECUTOR, BLOCKING_EXECUTOR);
   }

   private final AttributeSet attributes;
   private final Attribute<String> defaultCache;
   private final Attribute<String> name;
   private final Attribute<Boolean> statistics;
   private final Attribute<Boolean> zeroCapacityNode;
   private final boolean zeroCapacityAvailable;
   private final GlobalStateConfiguration globalState;
   private final GlobalJmxConfiguration jmx;
   private final GlobalMetricsConfiguration metrics;
   private final GlobalSecurityConfiguration security;
   private final SerializationConfiguration serialization;
   private final ShutdownConfiguration shutdown;
   private final ThreadsConfiguration threads;
   private final GlobalTracingConfiguration tracing;
   private final TransportConfiguration transport;
   private final Map<String, ContainerMemoryConfiguration> memoryContainers;


   CacheContainerConfiguration(AttributeSet attributes,
                               GlobalStateConfiguration globalState,
                               GlobalJmxConfiguration jmx,
                               GlobalMetricsConfiguration metrics,
                               GlobalSecurityConfiguration security,
                               SerializationConfiguration serialization,
                               ShutdownConfiguration shutdown,
                               ThreadsConfiguration threads,
                               GlobalTracingConfiguration tracing,
                               TransportConfiguration transport,
                               Features features, Map<String, ContainerMemoryConfiguration> memoryContainers) {
      this.attributes = attributes.checkProtection();
      this.defaultCache = attributes.attribute(DEFAULT_CACHE);
      this.name = attributes.attribute(NAME);
      this.statistics = attributes.attribute(STATISTICS);
      this.zeroCapacityNode = attributes.attribute(ZERO_CAPACITY_NODE);
      this.tracing = tracing;
      this.globalState = globalState;
      this.jmx = jmx;
      this.metrics = metrics;
      this.security = security;
      this.serialization = serialization;
      this.shutdown = shutdown;
      this.threads = threads;
      this.transport = transport;
      this.zeroCapacityAvailable = features.isAvailable(ZERO_CAPACITY_NODE_FEATURE);
      this.memoryContainers = memoryContainers;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String defaultCacheName() {
      return defaultCache.get();
   }

   public String cacheManagerName() {
      return name.get();
   }

   public boolean statistics() {
      return statistics.get();
   }

   public boolean getZeroCapacityNode() {
      return zeroCapacityAvailable && zeroCapacityNode.get();
   }

   public GlobalMetricsConfiguration metrics() {
      return metrics;
   }

   public GlobalTracingConfiguration tracing() {
      return tracing;
   }

   public GlobalJmxConfiguration jmx() {
      return jmx;
   }

   public TransportConfiguration transport() {
      return transport;
   }

   public GlobalSecurityConfiguration security() {
      return security;
   }

   public SerializationConfiguration serialization() {
      return serialization;
   }

   public ShutdownConfiguration shutdown() {
      return shutdown;
   }

   public String listenerExecutor() {
      return attributes.attribute(LISTENER_EXECUTOR).get();
   }

   public String expirationExecutor() {
      return attributes.attribute(EXPIRATION_EXECUTOR).get();
   }

   public String nonBlockingExecutor() {
      return attributes.attribute(NON_BLOCKING_EXECUTOR).get();
   }

   public String blockingExecutor() {
      return attributes.attribute(BLOCKING_EXECUTOR).get();
   }

   public GlobalStateConfiguration globalState() {
      return globalState;
   }

   public boolean isClustered() {
      return transport().transport() != null;
   }

   public ThreadsConfiguration threads() {
      return threads;
   }

   public ThreadPoolConfiguration expirationThreadPool() {
      return threads.expirationThreadPool();
   }

   public ThreadPoolConfiguration listenerThreadPool() {
      return threads.listenerThreadPool();
   }

   public ThreadPoolConfiguration persistenceThreadPool() {
      return threads.persistenceThreadPool();
   }

   public ThreadPoolConfiguration nonBlockingThreadPool() {
      return threads.nonBlockingThreadPool();
   }

   public ThreadPoolConfiguration blockingThreadPool() {
      return threads.blockingThreadPool();
   }

   public Map<String, ContainerMemoryConfiguration> containerMemoryConfiguration() {
      return memoryContainers;
   }

   @Override
   public String toString() {
      return "CacheContainerConfiguration{" +
            "zeroCapacityAvailable=" + zeroCapacityAvailable +
            ", threads=" + threads +
            ", metrics=" + metrics +
            ", jmx=" + jmx +
            ", transport=" + transport +
            ", security=" + security +
            ", serialization=" + serialization +
            ", globalState=" + globalState +
            ", shutdown=" + shutdown +
            ", attributes=" + attributes +
            '}';
   }
}

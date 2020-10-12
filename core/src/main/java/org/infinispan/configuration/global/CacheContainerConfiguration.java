package org.infinispan.configuration.global;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.util.Features;
import org.infinispan.configuration.parsing.Element;

/*
 * @since 10.0
 */
class CacheContainerConfiguration implements ConfigurationInfo {

   private static final String ZERO_CAPACITY_NODE_FEATURE = "zero-capacity-node";

   static final AttributeDefinition<String> DEFAULT_CACHE = AttributeDefinition.builder("defaultCache", null, String.class).immutable().build();
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", "DefaultCacheManager").immutable().build();
   static final AttributeDefinition<Boolean> STATISTICS = AttributeDefinition.builder("statistics", false).immutable().build();
   static final AttributeDefinition<Boolean> ZERO_CAPACITY_NODE = AttributeDefinition.builder("zeroCapacityNode", Boolean.FALSE).immutable().build();
   static final AttributeDefinition<String> ASYNC_EXECUTOR = AttributeDefinition.builder("asyncExecutor", "async-pool", String.class).immutable().build();
   static final AttributeDefinition<String> LISTENER_EXECUTOR = AttributeDefinition.builder("listenerExecutor", "listener-pool", String.class).immutable().build();
   static final AttributeDefinition<String> EXPIRATION_EXECUTOR = AttributeDefinition.builder("expirationExecutor", "expiration-pool", String.class).immutable().build();
   static final AttributeDefinition<String> PERSISTENCE_EXECUTOR = AttributeDefinition.builder("persistenceExecutor", "persistence-pool", String.class).immutable().build();
   static final AttributeDefinition<String> STATE_TRANSFER_EXECUTOR = AttributeDefinition.builder("stateTransferExecutor", "state-transfer-pool", String.class).immutable().build();
   static final AttributeDefinition<String> NON_BLOCKING_EXECUTOR = AttributeDefinition.builder("nonBlockingExecutor", "non-blocking-pool", String.class).immutable().build();
   static final AttributeDefinition<String> BLOCKING_EXECUTOR = AttributeDefinition.builder("blockingExecutor", "blocking-pool", String.class).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CacheContainerConfiguration.class, NAME, STATISTICS, ZERO_CAPACITY_NODE, DEFAULT_CACHE,
            ASYNC_EXECUTOR, LISTENER_EXECUTOR, EXPIRATION_EXECUTOR, PERSISTENCE_EXECUTOR, STATE_TRANSFER_EXECUTOR,
            NON_BLOCKING_EXECUTOR, BLOCKING_EXECUTOR);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.CACHE_CONTAINER.getLocalName());

   private final Attribute<String> defaultCache;
   private final Attribute<String> name;
   private final Attribute<Boolean> statistics;
   private final Attribute<Boolean> zeroCapacityNode;
   private final boolean zeroCapacityAvailable;

   private final ThreadsConfiguration threads;
   private final GlobalMetricsConfiguration metrics;
   private final GlobalJmxConfiguration jmx;
   private final TransportConfiguration transport;
   private final GlobalSecurityConfiguration security;
   private final SerializationConfiguration serialization;
   private final GlobalStateConfiguration globalState;
   private final ShutdownConfiguration shutdown;
   private final AttributeSet attributes;
   private final List<ConfigurationInfo> children;

   CacheContainerConfiguration(AttributeSet attributes,
                               ThreadsConfiguration threadsConfiguration,
                               GlobalMetricsConfiguration metrics,
                               GlobalJmxConfiguration jmx,
                               TransportConfiguration transport,
                               GlobalSecurityConfiguration security,
                               SerializationConfiguration serialization,
                               GlobalStateConfiguration globalState,
                               ShutdownConfiguration shutdown,
                               Features features) {
      this.attributes = attributes.checkProtection();
      this.defaultCache = attributes.attribute(DEFAULT_CACHE);
      this.name = attributes.attribute(NAME);
      this.statistics = attributes.attribute(STATISTICS);
      this.zeroCapacityNode = attributes.attribute(ZERO_CAPACITY_NODE);
      this.threads = threadsConfiguration;
      this.metrics = metrics;
      this.jmx = jmx;
      this.globalState = globalState;
      this.shutdown = shutdown;
      this.security = security;
      this.serialization = serialization;
      this.transport = transport;
      this.zeroCapacityAvailable = features.isAvailable(ZERO_CAPACITY_NODE_FEATURE);
      this.children = Arrays.asList(shutdown, transport, security, serialization, metrics, jmx, globalState);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return children;
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

   public GlobalJmxConfiguration jmx() {
      return jmx;
   }

   /**
    * @deprecated Since 10.1.3. Use {@link #jmx()} instead. This will be removed in next major version.
    */
   @Deprecated
   public GlobalJmxConfiguration globalJmxStatistics() {
      return jmx();
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

   @Deprecated
   public String asyncExecutor() {
      return attributes.attribute(ASYNC_EXECUTOR).get();
   }

   public String listenerExecutor() {
      return attributes.attribute(LISTENER_EXECUTOR).get();
   }

   public String expirationExecutor() {
      return attributes.attribute(EXPIRATION_EXECUTOR).get();
   }

   public String persistenceExecutor() {
      return attributes.attribute(PERSISTENCE_EXECUTOR).get();
   }

   /**
    * @deprecated Since 10.1, no longer used.
    */
   @Deprecated
   public String stateTransferExecutor() {
      return attributes.attribute(STATE_TRANSFER_EXECUTOR).get();
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

   /**
    * @return An empty {@code ThreadPoolConfiguration}.
    * @deprecated Since 10.1, no longer used.
    */
   @Deprecated
   public ThreadPoolConfiguration stateTransferThreadPool() {
      return threads.stateTransferThreadPool();
   }

   @Deprecated
   public ThreadPoolConfiguration asyncThreadPool() {
      return threads.asyncThreadPool();
   }

   public ThreadPoolConfiguration nonBlockingThreadPool() {
      return threads.nonBlockingThreadPool();
   }

   public ThreadPoolConfiguration blockingThreadPool() {
      return threads.blockingThreadPool();
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

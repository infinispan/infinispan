package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.ASYNC_EXECUTOR_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_CALLBACK_HANDLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_CLIENT_SUBJECT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_SERVER_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.BATCH_SIZE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CACHE_CONFIGURATION_SUFFIX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CACHE_MARSHALLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CACHE_NEAR_CACHE_MODE_SUFFIX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CACHE_PREFIX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CACHE_TEMPLATE_NAME_SUFFIX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CLIENT_INTELLIGENCE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CONNECT_TIMEOUT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CONTEXT_INITIALIZERS;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.DEFAULT_EXECUTOR_FACTORY_POOL_SIZE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.DNS_RESOLVER_MAX_TTL;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.DNS_RESOLVER_MIN_TTL;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.DNS_RESOLVER_NEGATIVE_TTL;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.FORCE_RETURN_VALUES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.HASH_FUNCTION_PREFIX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.JAVA_SERIAL_ALLOWLIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_STORE_FILE_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_STORE_PASSWORD;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.MARSHALLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.MAX_RETRIES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.REQUEST_BALANCING_STRATEGY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SASL_MECHANISM;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SASL_PROPERTIES_PREFIX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_FAILURE_TIMEOUT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SNI_HOST_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SO_TIMEOUT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SSL_CONTEXT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SSL_PROTOCOL;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.STATISTICS;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_KEEP_ALIVE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_NO_DELAY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRANSACTION_TIMEOUT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRUST_STORE_FILE_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRUST_STORE_PASSWORD;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.USE_AUTH;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.USE_SSL;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.TransportFactory;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.metrics.RemoteCacheManagerMetricsRegistry;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Features;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * Configuration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@BuiltBy(ConfigurationBuilder.class)
public class Configuration implements org.infinispan.api.configuration.Configuration {

   private final ExecutorFactoryConfiguration asyncExecutorFactory;
   private final Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory;
   private final WeakReference<ClassLoader> classLoader;
   private final ClientIntelligence clientIntelligence;
   private final ConnectionPoolConfiguration connectionPool;
   private final int connectionTimeout;
   private final Class<? extends ConsistentHash>[] consistentHashImpl;
   private final boolean forceReturnValues;
   private final Class<? extends Marshaller> marshallerClass;
   private final Marshaller marshaller;
   private final ProtocolVersion protocolVersion;
   private final List<ServerConfiguration> servers;
   private final int socketTimeout;
   private final SecurityConfiguration security;
   private final boolean tcpNoDelay;
   private final boolean tcpKeepAlive;
   private final int maxRetries;
   private final List<ClusterConfiguration> clusters;
   private final List<String> serialAllowList;
   private final int batchSize;
   private final ClassAllowList classAllowList;
   private final StatisticsConfiguration statistics;
   private final Features features;
   private final List<SerializationContextInitializer> contextInitializers;
   private final Map<String, RemoteCacheConfiguration> remoteCaches;
   private final TransportFactory transportFactory;
   private final boolean tracingPropagationEnabled;
   private final int dnsResolverMinTTL;
   private final int dnsResolverMaxTTL;
   private final int dnsResolverNegativeTTL;
   private final RemoteCacheManagerMetricsRegistry metricRegistry;
   private final int serverFailureTimeout;
   private final long transactionTimeout;

   public Configuration(ExecutorFactoryConfiguration asyncExecutorFactory, Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory, ClassLoader classLoader,
                        ClientIntelligence clientIntelligence, ConnectionPoolConfiguration connectionPool, int connectionTimeout, Class<? extends ConsistentHash>[] consistentHashImpl,
                        int dnsResolverMinTTL, int dnsResolverMaxTTL, int dnsResolverNegativeTTL,
                        boolean forceReturnValues,
                        Marshaller marshaller, Class<? extends Marshaller> marshallerClass,
                        ProtocolVersion protocolVersion, List<ServerConfiguration> servers, int socketTimeout, SecurityConfiguration security, boolean tcpNoDelay, boolean tcpKeepAlive,
                        int maxRetries,
                        List<ClusterConfiguration> clusters, List<String> serialAllowList, int batchSize, long transactionTimeout,
                        StatisticsConfiguration statistics, Features features,
                        List<SerializationContextInitializer> contextInitializers,
                        Map<String, RemoteCacheConfiguration> remoteCaches,
                        TransportFactory transportFactory, boolean tracingPropagationEnabled, RemoteCacheManagerMetricsRegistry metricRegistry,
                        int serverFailureTimeout) {
      this.asyncExecutorFactory = asyncExecutorFactory;
      this.balancingStrategyFactory = balancingStrategyFactory;
      this.maxRetries = maxRetries;
      this.classLoader = new WeakReference<>(classLoader);
      this.clientIntelligence = clientIntelligence;
      this.connectionPool = connectionPool;
      this.connectionTimeout = connectionTimeout;
      this.consistentHashImpl = consistentHashImpl;
      this.dnsResolverMinTTL = dnsResolverMinTTL;
      this.dnsResolverMaxTTL = dnsResolverMaxTTL;
      this.dnsResolverNegativeTTL = dnsResolverNegativeTTL;
      this.forceReturnValues = forceReturnValues;
      this.marshallerClass = marshallerClass;
      this.marshaller = marshaller;
      this.protocolVersion = protocolVersion;
      this.servers = List.copyOf(servers);
      this.socketTimeout = socketTimeout;
      this.security = security;
      this.tcpNoDelay = tcpNoDelay;
      this.tcpKeepAlive = tcpKeepAlive;
      this.clusters = clusters;
      this.serialAllowList = serialAllowList;
      this.classAllowList = new ClassAllowList(serialAllowList);
      this.batchSize = batchSize;
      this.transactionTimeout = transactionTimeout;
      this.statistics = statistics;
      this.features = features;
      this.contextInitializers = contextInitializers;
      this.remoteCaches = remoteCaches;
      this.transportFactory = transportFactory;
      this.tracingPropagationEnabled = tracingPropagationEnabled;
      this.metricRegistry = Objects.requireNonNullElse(metricRegistry, RemoteCacheManagerMetricsRegistry.DISABLED);
      this.serverFailureTimeout = serverFailureTimeout;
   }

   public ExecutorFactoryConfiguration asyncExecutorFactory() {
      return asyncExecutorFactory;
   }

   public Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory() {
      return balancingStrategyFactory;
   }

   @Deprecated(forRemoval=true)
   public ClassLoader classLoader() {
      return classLoader.get();
   }

   public ClientIntelligence clientIntelligence() {
      return clientIntelligence;
   }

   public ConnectionPoolConfiguration connectionPool() {
      return connectionPool;
   }

   public int connectionTimeout() {
      return connectionTimeout;
   }

   public Class<? extends ConsistentHash>[] consistentHashImpl() {
      return Arrays.copyOf(consistentHashImpl, consistentHashImpl.length);
   }

   public Class<? extends ConsistentHash> consistentHashImpl(int version) {
      return consistentHashImpl[version - 1];
   }

   public int dnsResolverMinTTL() {
      return dnsResolverMinTTL;
   }

   public int dnsResolverMaxTTL() {
      return dnsResolverMaxTTL;
   }

   public int dnsResolverNegativeTTL() {
      return dnsResolverNegativeTTL;
   }

   public boolean forceReturnValues() {
      return forceReturnValues;
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   public Class<? extends Marshaller> marshallerClass() {
      return marshallerClass;
   }

   public ProtocolVersion version() {
      return protocolVersion;
   }

   public List<ServerConfiguration> servers() {
      return servers;
   }

   public List<ClusterConfiguration> clusters() {
      return clusters;
   }

   public int socketTimeout() {
      return socketTimeout;
   }

   public SecurityConfiguration security() {
      return security;
   }

   public boolean tcpNoDelay() {
      return tcpNoDelay;
   }

   public boolean tcpKeepAlive() {
      return tcpKeepAlive;
   }

   public int maxRetries() {
      return maxRetries;
   }

   public List<String> serialAllowList() {
      return serialAllowList;
   }

   public ClassAllowList getClassAllowList() {
      return classAllowList;
   }

   public int batchSize() {
      return batchSize;
   }

   public Map<String, RemoteCacheConfiguration> remoteCaches() {
      return Collections.unmodifiableMap(remoteCaches);
   }

   /**
    * Create a new {@link RemoteCacheConfiguration}. This can be used to create additional configurations after a {@link org.infinispan.client.hotrod.RemoteCacheManager} has been initialized.
    *
    * @param name the name of the cache configuration to create
    * @param builderConsumer a {@link Consumer} which receives a {@link RemoteCacheConfigurationBuilder} and can apply the necessary configurations on it.
    * @return the {@link RemoteCacheConfiguration}
    * @throws IllegalArgumentException if a cache configuration with the same name already exists
    */
   public RemoteCacheConfiguration addRemoteCache(String name, Consumer<RemoteCacheConfigurationBuilder> builderConsumer) {
      return remoteCaches.compute(name, (ignore, existent) -> {
         if (existent != null)
            throw Log.HOTROD.duplicateCacheConfiguration(name);

         RemoteCacheConfigurationBuilder builder = new RemoteCacheConfigurationBuilder(null, name);
         builderConsumer.accept(builder);
         builder.validate();
         return builder.create();
      });
   }

   /**
    * Remove a {@link RemoteCacheConfiguration} from this {@link Configuration}. If the cache configuration doesn't exist, this method has no effect.
    * @param name the name of the {@link RemoteCacheConfiguration} to remove.
    */
   public void removeRemoteCache(String name) {
      remoteCaches.remove(name);
   }

   public StatisticsConfiguration statistics() {
      return statistics;
   }

   /**
    * see {@link ConfigurationBuilder#transactionTimeout(long, TimeUnit)},
    */
   public long transactionTimeout() {
      return transactionTimeout;
   }

   public Features features() {
      return features;
   }

   public List<SerializationContextInitializer> getContextInitializers() {
      return contextInitializers;
   }

   public TransportFactory transportFactory() {
      return transportFactory;
   }

   /**
    * OpenTelemetry tracing propagation will be activated if this property is true
    * and if the OpenTelemetry API jar is detected on the classpath.
    * By default, the property is true.
    *
    * @return if the tracing propagation is enabled
    */
   public boolean tracingPropagationEnabled() {
      return tracingPropagationEnabled;
   }

   /**
    * @return The {@link RemoteCacheManagerMetricsRegistry} implementation to use for metrics.
    */
   public RemoteCacheManagerMetricsRegistry metricRegistry() {
      return metricRegistry;
   }

   /**
    * Controls how long a server is marked as failed in milliseconds.
    * Default is 30_000 milliseconds or 30 seconds.
    * @return time in milliseconds
    */
   public int serverFailureTimeout() {
      return serverFailureTimeout;
   }

   @Override
   public String toString() {
      return "Configuration [asyncExecutorFactory=" + asyncExecutorFactory + ", balancingStrategyFactory=()->" + balancingStrategyFactory.get()
            + ",classLoader=" + classLoader + ", clientIntelligence=" + clientIntelligence + ", connectionPool="
            + connectionPool + ", connectionTimeout=" + connectionTimeout + ", consistentHashImpl=" + Arrays.toString(consistentHashImpl) + ", forceReturnValues="
            + forceReturnValues + ", marshallerClass=" + marshallerClass + ", marshaller=" + marshaller + ", protocolVersion="
            + protocolVersion + ", servers=" + servers + ", socketTimeout=" + socketTimeout + ", security=" + security + ", tcpNoDelay=" + tcpNoDelay + ", tcpKeepAlive=" + tcpKeepAlive
            + ", maxRetries=" + maxRetries
            + ", serialAllowList=" + serialAllowList
            + ", batchSize=" + batchSize
            + ", remoteCaches= " + remoteCaches
            + ", transactionTimeout=" + transactionTimeout
            + ", statistics=" + statistics
            + ", metricRegistry=" + metricRegistry
            + ", serverFailureTimeout=" + serverFailureTimeout
            + "]";
   }

   public Properties properties() {
      TypedProperties properties = new TypedProperties();
      if (asyncExecutorFactory().factoryClass() != null) {
         properties.setProperty(ASYNC_EXECUTOR_FACTORY, asyncExecutorFactory().factoryClass().getName());
         TypedProperties aefProps = asyncExecutorFactory().properties();
         if (aefProps.containsKey(DEFAULT_EXECUTOR_FACTORY_POOL_SIZE)) {
            properties.setProperty(DEFAULT_EXECUTOR_FACTORY_POOL_SIZE, aefProps.getProperty(DEFAULT_EXECUTOR_FACTORY_POOL_SIZE));
         }
      }
      properties.setProperty(REQUEST_BALANCING_STRATEGY, balancingStrategyFactory().get().getClass().getName());
      properties.setProperty(CLIENT_INTELLIGENCE, clientIntelligence().name());
      properties.setProperty(CONNECT_TIMEOUT, Integer.toString(connectionTimeout()));
      for (int i = 0; i < consistentHashImpl().length; i++) {
         int version = i + 1;
         if (consistentHashImpl(version) != null) {
            properties.setProperty(HASH_FUNCTION_PREFIX + "." + version,
                  consistentHashImpl(version).getName());
         }
      }
      properties.setProperty(FORCE_RETURN_VALUES, forceReturnValues());
      properties.setProperty(MARSHALLER, marshallerClass().getName());
      properties.setProperty(PROTOCOL_VERSION, version().toString());
      properties.setProperty(SO_TIMEOUT, socketTimeout());
      properties.setProperty(TCP_NO_DELAY, tcpNoDelay());
      properties.setProperty(TCP_KEEP_ALIVE, tcpKeepAlive());
      properties.setProperty(MAX_RETRIES, maxRetries());
      properties.setProperty(STATISTICS, statistics().enabled());
      properties.setProperty(SERVER_FAILURE_TIMEOUT, serverFailureTimeout());

      properties.setProperty(DNS_RESOLVER_MIN_TTL, dnsResolverMinTTL);
      properties.setProperty(DNS_RESOLVER_MAX_TTL, dnsResolverMaxTTL);
      properties.setProperty(DNS_RESOLVER_NEGATIVE_TTL, dnsResolverNegativeTTL);

      String servers = servers().stream()
            .map(server -> server.host() + ":" + server.port())
            .collect(Collectors.joining(";"));
      properties.setProperty(SERVER_LIST, servers);

      properties.setProperty(USE_SSL, Boolean.toString(security.ssl().enabled()));

      if (security.ssl().keyStoreFileName() != null)
         properties.setProperty(KEY_STORE_FILE_NAME, security.ssl().keyStoreFileName());

      if (security.ssl().keyStorePassword() != null)
         properties.setProperty(KEY_STORE_PASSWORD, new String(security.ssl().keyStorePassword()));

      if (security.ssl().trustStoreFileName() != null)
         properties.setProperty(TRUST_STORE_FILE_NAME, security.ssl().trustStoreFileName());

      if (security.ssl().trustStorePassword() != null)
         properties.setProperty(TRUST_STORE_PASSWORD, new String(security.ssl().trustStorePassword()));

      if (security.ssl().sniHostName() != null)
         properties.setProperty(SNI_HOST_NAME, security.ssl().sniHostName());

      if (security.ssl().protocol() != null)
         properties.setProperty(SSL_PROTOCOL, security.ssl().protocol());

      if (security.ssl().sslContext() != null)
         properties.put(SSL_CONTEXT, security.ssl().sslContext());

      properties.setProperty(USE_AUTH, Boolean.toString(security.authentication().enabled()));

      if (security.authentication().saslMechanism() != null)
         properties.setProperty(SASL_MECHANISM, security.authentication().saslMechanism());

      if (security.authentication().callbackHandler() != null)
         properties.put(AUTH_CALLBACK_HANDLER, security.authentication().callbackHandler());

      if (security.authentication().serverName() != null)
         properties.setProperty(AUTH_SERVER_NAME, security.authentication().serverName());

      if (security.authentication().clientSubject() != null)
         properties.put(AUTH_CLIENT_SUBJECT, security.authentication().clientSubject());

      for (Map.Entry<String, String> entry : security.authentication().saslProperties().entrySet())
         properties.setProperty(SASL_PROPERTIES_PREFIX + '.' + entry.getKey(), entry.getValue());

      properties.setProperty(JAVA_SERIAL_ALLOWLIST, String.join(",", serialAllowList));

      properties.setProperty(BATCH_SIZE, Integer.toString(batchSize));

      properties.setProperty(TRANSACTION_TIMEOUT, Long.toString(transactionTimeout));

      if (contextInitializers != null && !contextInitializers.isEmpty())
         properties.setProperty(CONTEXT_INITIALIZERS, contextInitializers.stream().map(sci -> sci.getClass().getName()).collect(Collectors.joining(",")));

      for (RemoteCacheConfiguration remoteCache : remoteCaches.values()) {
         String prefix = CACHE_PREFIX + remoteCache.name();
         if (remoteCache.templateName() != null) {
            properties.setProperty(prefix + CACHE_TEMPLATE_NAME_SUFFIX, remoteCache.templateName());
         }
         if (remoteCache.configuration() != null) {
            properties.setProperty(prefix + CACHE_CONFIGURATION_SUFFIX, remoteCache.configuration());
         }
         properties.setProperty(prefix + CACHE_NEAR_CACHE_MODE_SUFFIX, remoteCache.nearCacheMode().name());
         properties.setProperty(prefix + CACHE_NEAR_CACHE_MODE_SUFFIX, remoteCache.nearCacheMaxEntries());
         Marshaller marshaller = remoteCache.marshaller();
         if (marshaller != null) {
            properties.setProperty(prefix + CACHE_MARSHALLER, remoteCache.marshaller().getClass().getName());
         } else {
            Class<? extends Marshaller> marshallerClass = remoteCache.marshallerClass();
            if(marshallerClass != null) {
               properties.setProperty(prefix + CACHE_MARSHALLER, marshallerClass.getName());
            }
         }
      }

      return properties;
   }
}

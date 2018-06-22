package org.infinispan.client.hotrod.configuration;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Features;
import org.infinispan.commons.util.TypedProperties;

/**
 * Configuration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@BuiltBy(ConfigurationBuilder.class)
public class Configuration {

   private final ExecutorFactoryConfiguration asyncExecutorFactory;
   private final Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory;
   private final WeakReference<ClassLoader> classLoader;
   private final ClientIntelligence clientIntelligence;
   private final ConnectionPoolConfiguration connectionPool;
   private final int connectionTimeout;
   private final Class<? extends ConsistentHash>[] consistentHashImpl;
   private final boolean forceReturnValues;
   private final int keySizeEstimate;
   private final Class<? extends Marshaller> marshallerClass;
   private final Marshaller marshaller;
   private final ProtocolVersion protocolVersion;
   private final List<ServerConfiguration> servers;
   private final int socketTimeout;
   private final SecurityConfiguration security;
   private final boolean tcpNoDelay;
   private final boolean tcpKeepAlive;
   private final int valueSizeEstimate;
   private final int maxRetries;
   private final NearCacheConfiguration nearCache;
   private final List<ClusterConfiguration> clusters;
   private final List<String> serialWhitelist;
   private final int batchSize;
   private final ClassWhiteList classWhiteList;
   private final StatisticsConfiguration statistics;
   private final TransactionConfiguration transaction;
   private final Features features;
   private final SinglePortMode singlePort;

   Configuration(ExecutorFactoryConfiguration asyncExecutorFactory, Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory, ClassLoader classLoader,
                 ClientIntelligence clientIntelligence, ConnectionPoolConfiguration connectionPool, int connectionTimeout, Class<? extends ConsistentHash>[] consistentHashImpl, boolean forceReturnValues, int keySizeEstimate,
                 Marshaller marshaller, Class<? extends Marshaller> marshallerClass,
                 ProtocolVersion protocolVersion, List<ServerConfiguration> servers, SinglePortMode singlePort, int socketTimeout, SecurityConfiguration security, boolean tcpNoDelay, boolean tcpKeepAlive,
                 int valueSizeEstimate, int maxRetries, NearCacheConfiguration nearCache,
                 List<ClusterConfiguration> clusters, List<String> serialWhitelist, int batchSize,
                 TransactionConfiguration transaction, StatisticsConfiguration statistics, Features features) {
      this.asyncExecutorFactory = asyncExecutorFactory;
      this.balancingStrategyFactory = balancingStrategyFactory;
      this.maxRetries = maxRetries;
      this.classLoader = new WeakReference<>(classLoader);
      this.clientIntelligence = clientIntelligence;
      this.connectionPool = connectionPool;
      this.connectionTimeout = connectionTimeout;
      this.consistentHashImpl = consistentHashImpl;
      this.forceReturnValues = forceReturnValues;
      this.keySizeEstimate = keySizeEstimate;
      this.marshallerClass = marshallerClass;
      this.marshaller = marshaller;
      this.protocolVersion = protocolVersion;
      this.servers = Collections.unmodifiableList(servers);
      this.socketTimeout = socketTimeout;
      this.security = security;
      this.tcpNoDelay = tcpNoDelay;
      this.tcpKeepAlive = tcpKeepAlive;
      this.valueSizeEstimate = valueSizeEstimate;
      this.nearCache = nearCache;
      this.clusters = clusters;
      this.serialWhitelist = serialWhitelist;
      this.classWhiteList = new ClassWhiteList(serialWhitelist);
      this.batchSize = batchSize;
      this.transaction = transaction;
      this.statistics = statistics;
      this.singlePort = singlePort;
      this.features = features;
   }

   public ExecutorFactoryConfiguration asyncExecutorFactory() {
      return asyncExecutorFactory;
   }

   /**
    * Use {@link #balancingStrategyFactory()} instead.
    *
    * @deprecated since 9.3
    */
   @Deprecated
   public Class<? extends org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy> balancingStrategyClass() {
      FailoverRequestBalancingStrategy strategy = balancingStrategyFactory.get();
      if (org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy.class.isInstance(strategy)) {
         return (Class<? extends org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy>) strategy.getClass();
      } else {
         return org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy.class;
      }
   }

   /**
    * Use {@link #balancingStrategyFactory()} instead.
    *
    * @deprecated since 9.3
    */
   @Deprecated
   public org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy balancingStrategy() {
      FailoverRequestBalancingStrategy strategy = balancingStrategyFactory.get();
      if (org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy.class.isInstance(strategy)) {
         return (org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy) strategy;
      } else {
         return null;
      }
   }

   public Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory() {
      return balancingStrategyFactory;
   }

   @Deprecated
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

   public boolean forceReturnValues() {
      return forceReturnValues;
   }

   public int keySizeEstimate() {
      return keySizeEstimate;
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   public Class<? extends Marshaller> marshallerClass() {
      return marshallerClass;
   }

   public NearCacheConfiguration nearCache() {
      return nearCache;
   }

   /**
    * @deprecated Use {@link Configuration#version()} instead.
    */
   @Deprecated
   public String protocolVersion() {
      return protocolVersion.toString();
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

   @Deprecated
   public Class<? extends TransportFactory> transportFactory() {
      return TransportFactory.class;
   }

   public int valueSizeEstimate() {
      return valueSizeEstimate;
   }

   public int maxRetries() {
      return maxRetries;
   }

   public List<String> serialWhitelist() {
      return serialWhitelist;
   }

   public ClassWhiteList getClassWhiteList() {
      return classWhiteList;
   }

   public int batchSize() {
      return batchSize;
   }

   public StatisticsConfiguration statistics() {
      return statistics;
   }

   public TransactionConfiguration transaction() {
      return transaction;
   }

   public Features features() {
      return features;
   }

   @Override
   public String toString() {
      return "Configuration [asyncExecutorFactory=" + asyncExecutorFactory + ", balancingStrategyFactory=()->" + balancingStrategyFactory.get()
            + ",classLoader=" + classLoader + ", clientIntelligence=" + clientIntelligence + ", connectionPool="
            + connectionPool + ", connectionTimeout=" + connectionTimeout + ", consistentHashImpl=" + Arrays.toString(consistentHashImpl) + ", forceReturnValues="
            + forceReturnValues + ", keySizeEstimate=" + keySizeEstimate + ", marshallerClass=" + marshallerClass + ", marshaller=" + marshaller + ", protocolVersion="
            + protocolVersion + ", servers=" + servers + ", socketTimeout=" + socketTimeout + ", security=" + security + ", tcpNoDelay=" + tcpNoDelay + ", tcpKeepAlive=" + tcpKeepAlive
            + ", valueSizeEstimate=" + valueSizeEstimate + ", maxRetries=" + maxRetries
            + ", serialWhiteList=" + serialWhitelist
            + ", batchSize=" + batchSize
            + ", nearCache=" + nearCache
            + ", transaction=" + transaction
            + ", statistics=" + statistics
            + "]";
   }

   public Properties properties() {
      TypedProperties properties = new TypedProperties();
      if (asyncExecutorFactory().factoryClass() != null) {
         properties.setProperty(ConfigurationProperties.ASYNC_EXECUTOR_FACTORY, asyncExecutorFactory().factoryClass().getName());
         TypedProperties aefProps = asyncExecutorFactory().properties();
         for (String key : Arrays.asList(ConfigurationProperties.DEFAULT_EXECUTOR_FACTORY_POOL_SIZE, ConfigurationProperties.DEFAULT_EXECUTOR_FACTORY_QUEUE_SIZE)) {
            if (aefProps.containsKey(key)) {
               properties.setProperty(key, aefProps.getProperty(key));
            }
         }
      }
      properties.setProperty(ConfigurationProperties.REQUEST_BALANCING_STRATEGY, balancingStrategyFactory().get().getClass().getName());
      properties.setProperty(ConfigurationProperties.CLIENT_INTELLIGENCE, clientIntelligence().name());
      properties.setProperty(ConfigurationProperties.CONNECT_TIMEOUT, Integer.toString(connectionTimeout()));
      for (int i = 0; i < consistentHashImpl().length; i++) {
         int version = i + 1;
         if (consistentHashImpl(version) != null) {
            properties.setProperty(ConfigurationProperties.HASH_FUNCTION_PREFIX + "." + version,
                  consistentHashImpl(version).getName());
         }
      }
      properties.setProperty(ConfigurationProperties.FORCE_RETURN_VALUES, forceReturnValues());
      properties.setProperty(ConfigurationProperties.KEY_SIZE_ESTIMATE, keySizeEstimate());
      properties.setProperty(ConfigurationProperties.MARSHALLER, marshallerClass().getName());
      properties.setProperty(ConfigurationProperties.PROTOCOL_VERSION, version().toString());
      properties.setProperty(ConfigurationProperties.SO_TIMEOUT, socketTimeout());
      properties.setProperty(ConfigurationProperties.TCP_NO_DELAY, tcpNoDelay());
      properties.setProperty(ConfigurationProperties.TCP_KEEP_ALIVE, tcpKeepAlive());
      properties.setProperty(ConfigurationProperties.VALUE_SIZE_ESTIMATE, valueSizeEstimate());
      properties.setProperty(ConfigurationProperties.MAX_RETRIES, maxRetries());
      properties.setProperty(ConfigurationProperties.STATISTICS, statistics().enabled());

      properties.setProperty(ConfigurationProperties.CONNECTION_POOL_EXHAUSTED_ACTION, connectionPool().exhaustedAction().name());
      properties.setProperty("exhaustedAction", connectionPool().exhaustedAction().ordinal());
      properties.setProperty(ConfigurationProperties.CONNECTION_POOL_MAX_ACTIVE, connectionPool().maxActive());
      properties.setProperty("maxActive", connectionPool().maxActive());
      properties.setProperty(ConfigurationProperties.CONNECTION_POOL_MAX_WAIT, connectionPool().maxWait());
      properties.setProperty("maxWait", connectionPool().maxWait());
      properties.setProperty(ConfigurationProperties.CONNECTION_POOL_MIN_IDLE, connectionPool().minIdle());
      properties.setProperty("minIdle", connectionPool().minIdle());
      properties.setProperty(ConfigurationProperties.CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME, connectionPool().minEvictableIdleTime());
      properties.setProperty("minEvictableIdleTimeMillis", connectionPool().minEvictableIdleTime());
      properties.setProperty(ConfigurationProperties.CONNECTION_POOL_MAX_PENDING_REQUESTS, connectionPool().maxPendingRequests());

      // Deprecated properties
      properties.setProperty("maxIdle", connectionPool().maxIdle());
      properties.setProperty("maxTotal", connectionPool().maxTotal());
      properties.setProperty("numTestsPerEvictionRun", connectionPool().numTestsPerEvictionRun());
      properties.setProperty("timeBetweenEvictionRunsMillis", connectionPool().timeBetweenEvictionRuns());
      properties.setProperty("lifo", connectionPool().lifo());
      properties.setProperty("testOnBorrow", connectionPool().testOnBorrow());
      properties.setProperty("testOnReturn", connectionPool().testOnReturn());
      properties.setProperty("testWhileIdle", connectionPool().testWhileIdle());

      StringBuilder servers = new StringBuilder();
      for (ServerConfiguration server : servers()) {
         if (servers.length() > 0) {
            servers.append(";");
         }
         servers.append(server.host()).append(":").append(server.port());
      }
      properties.setProperty(ConfigurationProperties.SERVER_LIST, servers.toString());

      properties.setProperty(ConfigurationProperties.SINGLE_PORT, singlePort.name());

      properties.setProperty(ConfigurationProperties.USE_SSL, Boolean.toString(security.ssl().enabled()));

      if (security.ssl().keyStoreFileName() != null)
         properties.setProperty(ConfigurationProperties.KEY_STORE_FILE_NAME, security.ssl().keyStoreFileName());

      if (security.ssl().keyStorePassword() != null)
         properties.setProperty(ConfigurationProperties.KEY_STORE_PASSWORD, new String(security.ssl().keyStorePassword()));

      if (security.ssl().keyStoreCertificatePassword() != null)
         properties.setProperty(ConfigurationProperties.KEY_STORE_CERTIFICATE_PASSWORD, new String(security.ssl().keyStoreCertificatePassword()));

      if (security.ssl().trustStoreFileName() != null)
         properties.setProperty(ConfigurationProperties.TRUST_STORE_FILE_NAME, security.ssl().trustStoreFileName());

      if (security.ssl().trustStorePassword() != null)
         properties.setProperty(ConfigurationProperties.TRUST_STORE_PASSWORD, new String(security.ssl().trustStorePassword()));

      if (security.ssl().sniHostName() != null)
         properties.setProperty(ConfigurationProperties.SNI_HOST_NAME, security.ssl().sniHostName());

      if (security.ssl().protocol() != null)
         properties.setProperty(ConfigurationProperties.SSL_PROTOCOL, security.ssl().protocol());

      if (security.ssl().sslContext() != null)
         properties.put(ConfigurationProperties.SSL_CONTEXT, security.ssl().sslContext());

      properties.setProperty(ConfigurationProperties.USE_AUTH, Boolean.toString(security.authentication().enabled()));

      if (security.authentication().saslMechanism() != null)
         properties.setProperty(ConfigurationProperties.SASL_MECHANISM, security.authentication().saslMechanism());

      if (security.authentication().callbackHandler() != null)
         properties.put(ConfigurationProperties.AUTH_CALLBACK_HANDLER, security.authentication().callbackHandler());

      if (security.authentication().serverName() != null)
         properties.setProperty(ConfigurationProperties.AUTH_SERVER_NAME, security.authentication().serverName());

      if (security.authentication().clientSubject() != null)
         properties.put(ConfigurationProperties.AUTH_CLIENT_SUBJECT, security.authentication().clientSubject());

      for (Map.Entry<String, String> entry : security.authentication().saslProperties().entrySet())
         properties.setProperty(ConfigurationProperties.SASL_PROPERTIES_PREFIX + '.' + entry.getKey(), entry.getValue());

      properties.setProperty(ConfigurationProperties.JAVA_SERIAL_WHITELIST, String.join(",", serialWhitelist));

      properties.setProperty(ConfigurationProperties.BATCH_SIZE, Integer.toString(batchSize));

      transaction.toProperties(properties);

      properties.setProperty(ConfigurationProperties.NEAR_CACHE_MODE, nearCache.mode().name());
      properties.setProperty(ConfigurationProperties.NEAR_CACHE_MAX_ENTRIES, Integer.toString(nearCache.maxEntries()));
      if (nearCache.cacheNamePattern() != null)
         properties.setProperty(ConfigurationProperties.NEAR_CACHE_NAME_PATTERN, nearCache.cacheNamePattern().pattern());

      return properties;
   }

   public SinglePortMode getSinglePort() {
      return singlePort;
   }
}

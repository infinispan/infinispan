package org.infinispan.client.hotrod.configuration;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.TypedProperties;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV2;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.Util;

/**
 * ConfigurationBuilder used to generate immutable {@link Configuration} objects to pass to the
 * {@link RemoteCacheManager#RemoteCacheManager(Configuration)} constructor.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class ConfigurationBuilder implements ConfigurationChildBuilder, Builder<Configuration> {

   private static final Log log = LogFactory.getLog(ConfigurationBuilder.class, Log.class);

   // Match IPv4 (host:port) or IPv6 ([host]:port) addresses
   private static final Pattern ADDRESS_PATTERN = Pattern
         .compile("(\\[([0-9A-Fa-f:]+)\\]|([^:/?#]*))(?::(\\d*))?");

   private WeakReference<ClassLoader> classLoader;
   private final ExecutorFactoryConfigurationBuilder asyncExecutorFactory;
   private Class<? extends FailoverRequestBalancingStrategy> balancingStrategyClass = RoundRobinBalancingStrategy.class;
   private FailoverRequestBalancingStrategy balancingStrategy;
   private ClientIntelligence clientIntelligence = ClientIntelligence.getDefault();
   private final ConnectionPoolConfigurationBuilder connectionPool;
   private int connectionTimeout = ConfigurationProperties.DEFAULT_CONNECT_TIMEOUT;
   @SuppressWarnings("unchecked")
   private final Class<? extends ConsistentHash> consistentHashImpl[] = new Class[] {
         null, ConsistentHashV2.class, SegmentConsistentHash.class
   };
   private boolean forceReturnValues;
   private int keySizeEstimate = ConfigurationProperties.DEFAULT_KEY_SIZE;
   private Class<? extends Marshaller> marshallerClass;
   private Marshaller marshaller;
   private ProtocolVersion protocolVersion = ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
   private final List<ServerConfigurationBuilder> servers = new ArrayList<ServerConfigurationBuilder>();
   private int socketTimeout = ConfigurationProperties.DEFAULT_SO_TIMEOUT;
   private final SecurityConfigurationBuilder security;
   private boolean tcpNoDelay = true;
   private boolean tcpKeepAlive = false;
   private Class<? extends TransportFactory> transportFactory = TcpTransportFactory.class;
   private int valueSizeEstimate = ConfigurationProperties.DEFAULT_VALUE_SIZE;
   private int maxRetries = ConfigurationProperties.DEFAULT_MAX_RETRIES;
   private final NearCacheConfigurationBuilder nearCache;
   private final List<String> whiteListRegExs = new ArrayList<>();
   private int batchSize = ConfigurationProperties.DEFAULT_BATCH_SIZE;

   private final List<ClusterConfigurationBuilder> clusters = new ArrayList<ClusterConfigurationBuilder>();

   public ConfigurationBuilder() {
      this.classLoader = new WeakReference<>(Thread.currentThread().getContextClassLoader());
      this.connectionPool = new ConnectionPoolConfigurationBuilder(this);
      this.asyncExecutorFactory = new ExecutorFactoryConfigurationBuilder(this);
      this.security = new SecurityConfigurationBuilder(this);
      this.nearCache = new NearCacheConfigurationBuilder(this);
   }

   @Override
   public ServerConfigurationBuilder addServer() {
      ServerConfigurationBuilder builder = new ServerConfigurationBuilder(this);
      this.servers.add(builder);
      return builder;
   }

   @Override
   public ClusterConfigurationBuilder addCluster(String clusterName) {
      ClusterConfigurationBuilder builder = new ClusterConfigurationBuilder(this, clusterName);
      this.clusters.add(builder);
      return builder;
   }

   @Override
   public ConfigurationBuilder addServers(String servers) {
      for (String server : servers.split(";")) {
         Matcher matcher = ADDRESS_PATTERN.matcher(server.trim());
         if (matcher.matches()) {
            String v6host = matcher.group(2);
            String v4host = matcher.group(3);
            String host = v6host != null ? v6host : v4host;
            String portString = matcher.group(4);
            int port = portString == null
                  ? ConfigurationProperties.DEFAULT_HOTROD_PORT
                  : Integer.parseInt(portString);
            this.addServer().host(host).port(port);
         } else {
            throw log.parseErrorServerAddress(server);
         }

      }
      return this;
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncExecutorFactory() {
      return this.asyncExecutorFactory;
   }

   @Override
   public ConfigurationBuilder balancingStrategy(String balancingStrategy) {
      this.balancingStrategyClass = Util.loadClass(balancingStrategy, this.classLoader());
      return this;
   }

   @Override
   public ConfigurationBuilder balancingStrategy(FailoverRequestBalancingStrategy balancingStrategy) {
      this.balancingStrategy = balancingStrategy;
      return this;
   }

   @Override
   public ConfigurationBuilder balancingStrategy(Class<? extends FailoverRequestBalancingStrategy> balancingStrategy) {
      this.balancingStrategyClass = balancingStrategy;
      return this;
   }

   @Override
   public ConfigurationBuilder classLoader(ClassLoader cl) {
      this.classLoader = new WeakReference<>(cl);
      return this;
   }

   ClassLoader classLoader() {
      return classLoader != null ? classLoader.get() : null;
   }

   @Override
   public ConfigurationBuilder clientIntelligence(ClientIntelligence clientIntelligence) {
      this.clientIntelligence = clientIntelligence;
      return this;
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return connectionPool;
   }

   @Override
   public ConfigurationBuilder connectionTimeout(int connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      return this;
   }

   @Override
   public ConfigurationBuilder consistentHashImpl(int version, Class<? extends ConsistentHash> consistentHashClass) {
      if (version == 1) {
         log.warn("Hash function version 1 is no longer supported.");
      } else {
         this.consistentHashImpl[version - 1] = consistentHashClass;
      }
      return this;
   }

   @Override
   public ConfigurationBuilder consistentHashImpl(int version, String consistentHashClass) {
      if (version == 1) {
         log.warn("Hash function version 1 is no longer supported.");
      } else {
         this.consistentHashImpl[version - 1] = Util.loadClass(consistentHashClass, classLoader());
      }
      return this;
   }

   @Override
   public ConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      this.forceReturnValues = forceReturnValues;
      return this;
   }

   @Override
   public ConfigurationBuilder keySizeEstimate(int keySizeEstimate) {
      this.keySizeEstimate = keySizeEstimate;
      return this;
   }

   @Override
   public ConfigurationBuilder marshaller(String marshaller) {
      this.marshallerClass = Util.loadClass(marshaller, this.classLoader());
      return this;
   }

   @Override
   public ConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller) {
      this.marshallerClass = marshaller;
      return this;
   }

   @Override
   public ConfigurationBuilder marshaller(Marshaller marshaller) {
      this.marshaller = marshaller;
      return this;
   }

   public NearCacheConfigurationBuilder nearCache() {
      return nearCache;
   }

   /**
    * @deprecated Use {@link ConfigurationBuilder#version(ProtocolVersion)} instead.
    */
   @Deprecated
   @Override
   public ConfigurationBuilder protocolVersion(String protocolVersion) {
      this.protocolVersion = ProtocolVersion.parseVersion(protocolVersion);
      return this;
   }

   @Override
   public ConfigurationBuilder version(ProtocolVersion protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
   }

   @Override
   public SecurityConfigurationBuilder security() {
      return security;
   }

   @Override
   public ConfigurationBuilder socketTimeout(int socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
   }

   @Override
   public ConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      this.tcpNoDelay = tcpNoDelay;
      return this;
   }

   @Override
   public ConfigurationBuilder tcpKeepAlive(boolean keepAlive) {
      this.tcpKeepAlive = keepAlive;
      return this;
   }

   @Override
   public ConfigurationBuilder transportFactory(String transportFactory) {
      this.transportFactory = Util.loadClass(transportFactory, this.classLoader());
      return this;
   }

   @Override
   public ConfigurationBuilder transportFactory(Class<? extends TransportFactory> transportFactory) {
      this.transportFactory = transportFactory;
      return this;
   }

   @Override
   public ConfigurationBuilder valueSizeEstimate(int valueSizeEstimate) {
      this.valueSizeEstimate = valueSizeEstimate;
      return this;
   }

   @Override
   public ConfigurationBuilder maxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
   }

   @Override
   public ConfigurationBuilder addJavaSerialWhiteList(String... regEx) {
      this.whiteListRegExs.addAll(Arrays.asList(regEx));
      return this;
   }

   @Override
   public ConfigurationBuilder batchSize(int batchSize) {
      if (batchSize <= 0) {
         throw new IllegalArgumentException("batchSize must be greater than 0");
      }
      this.batchSize = batchSize;
      return this;
   }

   @Override
   public ConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);

      if (typed.containsKey(ConfigurationProperties.ASYNC_EXECUTOR_FACTORY)) {
         this.asyncExecutorFactory().factoryClass(typed.getProperty(ConfigurationProperties.ASYNC_EXECUTOR_FACTORY, null, true));
      }
      this.asyncExecutorFactory().withExecutorProperties(typed);
      this.balancingStrategy(typed.getProperty(ConfigurationProperties.REQUEST_BALANCING_STRATEGY, balancingStrategyClass.getName(), true));
      this.clientIntelligence(typed.getEnumProperty(ConfigurationProperties.CLIENT_INTELLIGENCE, ClientIntelligence.class, ClientIntelligence.getDefault(), true));
      this.connectionPool.withPoolProperties(typed);
      this.connectionTimeout(typed.getIntProperty(ConfigurationProperties.CONNECT_TIMEOUT, connectionTimeout, true));
      if (typed.containsKey(ConfigurationProperties.HASH_FUNCTION_PREFIX + ".1")) {
         log.warn("Hash function version 1 is no longer supported");
      }
      for (int i = 0; i < consistentHashImpl.length; i++) {
         if (consistentHashImpl[i] != null) {
            int version = i + 1;
            this.consistentHashImpl(version,
                  typed.getProperty(ConfigurationProperties.HASH_FUNCTION_PREFIX + "." + version,
                        consistentHashImpl[i].getName(), true));
         }
      }
      this.forceReturnValues(typed.getBooleanProperty(ConfigurationProperties.FORCE_RETURN_VALUES, forceReturnValues, true));
      this.keySizeEstimate(typed.getIntProperty(ConfigurationProperties.KEY_SIZE_ESTIMATE, keySizeEstimate, true));
      if (typed.containsKey(ConfigurationProperties.MARSHALLER)) {
         this.marshaller(typed.getProperty(ConfigurationProperties.MARSHALLER, null, true));
      }
      this.version(ProtocolVersion.parseVersion(typed.getProperty(ConfigurationProperties.PROTOCOL_VERSION, protocolVersion.toString(), true)));
      String serverList = typed.getProperty(ConfigurationProperties.SERVER_LIST, null, true);
      if (serverList != null) {
         this.servers.clear();
         this.addServers(serverList);
      }
      this.socketTimeout(typed.getIntProperty(ConfigurationProperties.SO_TIMEOUT, socketTimeout, true));
      this.tcpNoDelay(typed.getBooleanProperty(ConfigurationProperties.TCP_NO_DELAY, tcpNoDelay, true));
      this.tcpKeepAlive(typed.getBooleanProperty(ConfigurationProperties.TCP_KEEP_ALIVE, tcpKeepAlive, true));
      if (typed.containsKey(ConfigurationProperties.TRANSPORT_FACTORY)) {
         this.transportFactory(typed.getProperty(ConfigurationProperties.TRANSPORT_FACTORY, null, true));
      }
      this.valueSizeEstimate(typed.getIntProperty(ConfigurationProperties.VALUE_SIZE_ESTIMATE, valueSizeEstimate, true));
      this.maxRetries(typed.getIntProperty(ConfigurationProperties.MAX_RETRIES, maxRetries, true));
      this.security.ssl().withProperties(properties);
      this.security.authentication().withProperties(properties);

      String serialWhitelist = typed.getProperty(ConfigurationProperties.JAVA_SERIAL_WHITELIST);
      if (serialWhitelist != null) {
         String[] classes = serialWhitelist.split(",");
         Collections.addAll(this.whiteListRegExs, classes);
      }

      this.batchSize(typed.getIntProperty(ConfigurationProperties.BATCH_SIZE, batchSize, true));

      return this;
   }

   @Override
   public void validate() {
      connectionPool.validate();
      asyncExecutorFactory.validate();
      security.validate();
      nearCache.validate();
      if (maxRetries < 0) {
         throw log.invalidMaxRetries(maxRetries);
      }
      Set<String> clusterNameSet = new HashSet<String>(clusters.size());
      for (ClusterConfigurationBuilder clusterConfigBuilder : clusters) {
         if (!clusterNameSet.add(clusterConfigBuilder.getClusterName())) {
            throw log.duplicateClusterDefinition(clusterConfigBuilder.getClusterName());
         }
         clusterConfigBuilder.validate();
      }
   }

   @Override
   public Configuration create() {
      List<ServerConfiguration> servers = new ArrayList<ServerConfiguration>();
      if (this.servers.size() > 0)
         for (ServerConfigurationBuilder server : this.servers) {
            servers.add(server.create());
         }
      else {
         servers.add(new ServerConfiguration("127.0.0.1", ConfigurationProperties.DEFAULT_HOTROD_PORT));
      }

      List<ClusterConfiguration> serverClusterConfigs = clusters.stream()
         .map(ClusterConfigurationBuilder::create).collect(Collectors.toList());
      if (marshaller == null && marshallerClass == null) {
         marshallerClass = GenericJBossMarshaller.class;
      }

      return new Configuration(asyncExecutorFactory.create(), balancingStrategyClass, balancingStrategy, classLoader == null ? null : classLoader.get(), clientIntelligence, connectionPool.create(), connectionTimeout,
            consistentHashImpl, forceReturnValues, keySizeEstimate, marshaller, marshallerClass, protocolVersion, servers, socketTimeout, security.create(), tcpNoDelay, tcpKeepAlive, transportFactory,
            valueSizeEstimate, maxRetries, nearCache.create(), serverClusterConfigs, whiteListRegExs, batchSize);
   }

   @Override
   public Configuration build() {
      return build(true);
   }

   public Configuration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public ConfigurationBuilder read(Configuration template) {
      this.classLoader = new WeakReference<ClassLoader>(template.classLoader());
      this.asyncExecutorFactory.read(template.asyncExecutorFactory());
      this.balancingStrategyClass = template.balancingStrategyClass();
      this.balancingStrategy = template.balancingStrategy();
      this.connectionPool.read(template.connectionPool());
      this.connectionTimeout = template.connectionTimeout();
      for (int i = 0; i < consistentHashImpl.length; i++) {
         this.consistentHashImpl[i] = template.consistentHashImpl(i + 1);
      }
      this.forceReturnValues = template.forceReturnValues();
      this.keySizeEstimate = template.keySizeEstimate();
      this.marshaller = template.marshaller();
      this.marshallerClass = template.marshallerClass();
      this.protocolVersion = template.version();
      this.servers.clear();
      for (ServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }
      this.socketTimeout = template.socketTimeout();
      this.security.read(template.security());
      this.tcpNoDelay = template.tcpNoDelay();
      this.tcpKeepAlive = template.tcpKeepAlive();
      this.transportFactory = template.transportFactory();
      this.valueSizeEstimate = template.valueSizeEstimate();
      this.maxRetries = template.maxRetries();
      this.nearCache.read(template.nearCache());
      this.whiteListRegExs.addAll(template.serialWhitelist());

      return this;
   }
}

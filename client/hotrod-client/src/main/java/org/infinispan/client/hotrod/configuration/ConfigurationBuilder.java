package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.HotRodURI;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV2;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Features;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * <p>ConfigurationBuilder used to generate immutable {@link Configuration} objects to pass to the
 * {@link RemoteCacheManager#RemoteCacheManager(Configuration)} constructor.</p>
 *
 * <p>If you prefer to configure the client declaratively, see {@link org.infinispan.client.hotrod.configuration}</p>
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class ConfigurationBuilder implements ConfigurationChildBuilder, Builder<Configuration> {

   private static final Log log = LogFactory.getLog(ConfigurationBuilder.class, Log.class);

   // Match IPv4 (host:port) or IPv6 ([host]:port) addresses
   private static final Pattern ADDRESS_PATTERN = Pattern
         .compile("(\\[([0-9A-Fa-f:]+)\\]|([^:/?#]*))(?::(\\d*))?");
   private static final int CACHE_PREFIX_LENGTH = ConfigurationProperties.CACHE_PREFIX.length();

   private WeakReference<ClassLoader> classLoader;
   private final ExecutorFactoryConfigurationBuilder asyncExecutorFactory;
   private Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory = RoundRobinBalancingStrategy::new;
   private ClientIntelligence clientIntelligence = ClientIntelligence.getDefault();
   private final ConnectionPoolConfigurationBuilder connectionPool;
   private int connectionTimeout = ConfigurationProperties.DEFAULT_CONNECT_TIMEOUT;
   @SuppressWarnings("unchecked")
   private final Class<? extends ConsistentHash> consistentHashImpl[] = new Class[]{
         null, ConsistentHashV2.class, SegmentConsistentHash.class
   };
   private boolean forceReturnValues;
   private int keySizeEstimate = ConfigurationProperties.DEFAULT_KEY_SIZE;
   private Class<? extends Marshaller> marshallerClass;
   private Marshaller marshaller;
   private ProtocolVersion protocolVersion = ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
   private final List<ServerConfigurationBuilder> servers = new ArrayList<>();
   private int socketTimeout = ConfigurationProperties.DEFAULT_SO_TIMEOUT;
   private final SecurityConfigurationBuilder security;
   private boolean tcpNoDelay = true;
   private boolean tcpKeepAlive = false;
   private int valueSizeEstimate = ConfigurationProperties.DEFAULT_VALUE_SIZE;
   private int maxRetries = ConfigurationProperties.DEFAULT_MAX_RETRIES;
   private final NearCacheConfigurationBuilder nearCache;
   private final List<String> whiteListRegExs = new ArrayList<>();
   private int batchSize = ConfigurationProperties.DEFAULT_BATCH_SIZE;
   private final TransactionConfigurationBuilder transaction;
   private final StatisticsConfigurationBuilder statistics;
   private final List<ClusterConfigurationBuilder> clusters = new ArrayList<>();
   private Features features;
   private final List<SerializationContextInitializer> contextInitializers = new ArrayList<>();
   private Map<String, RemoteCacheConfigurationBuilder> remoteCacheBuilders;

   public ConfigurationBuilder() {
      this.classLoader = new WeakReference<>(Thread.currentThread().getContextClassLoader());
      this.connectionPool = new ConnectionPoolConfigurationBuilder(this);
      this.asyncExecutorFactory = new ExecutorFactoryConfigurationBuilder(this);
      this.security = new SecurityConfigurationBuilder(this);
      this.nearCache = new NearCacheConfigurationBuilder(this);
      this.transaction = new TransactionConfigurationBuilder(this);
      this.statistics = new StatisticsConfigurationBuilder(this);
      this.remoteCacheBuilders = new HashMap<>();
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
      parseServers(servers, (host, port) -> addServer().host(host).port(port));
      return this;
   }

   public static final void parseServers(String servers, BiConsumer<String, Integer> c) {
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
            c.accept(host, port);
         } else {
            throw HOTROD.parseErrorServerAddress(server);
         }

      }
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncExecutorFactory() {
      return this.asyncExecutorFactory;
   }

   @Override
   public ConfigurationBuilder balancingStrategy(String balancingStrategy) {
      this.balancingStrategyFactory = () -> Util.getInstance(balancingStrategy, this.classLoader());
      return this;
   }

   @Deprecated
   @Override
   public ConfigurationBuilder balancingStrategy(FailoverRequestBalancingStrategy balancingStrategy) {
      this.balancingStrategyFactory = () -> balancingStrategy;
      return this;
   }

   @Override
   public ConfigurationBuilder balancingStrategy(Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory) {
      this.balancingStrategyFactory = balancingStrategyFactory;
      return this;
   }

   @Override
   public ConfigurationBuilder balancingStrategy(Class<? extends FailoverRequestBalancingStrategy> balancingStrategy) {
      this.balancingStrategyFactory = () -> Util.getInstance(balancingStrategy);
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
      this.marshallerClass = marshaller == null ? null : marshaller.getClass();
      return this;
   }

   @Override
   public ConfigurationBuilder addContextInitializer(String contextInitializer) {
      SerializationContextInitializer sci = Util.getInstance(contextInitializer, this.classLoader());
      return addContextInitializers(sci);
   }

   @Override
   public ConfigurationBuilder addContextInitializer(SerializationContextInitializer contextInitializer) {
      if (contextInitializer != null)
         this.contextInitializers.add(contextInitializer);
      return this;
   }

   @Override
   public ConfigurationBuilder addContextInitializers(SerializationContextInitializer... contextInitializers) {
      this.contextInitializers.addAll(Arrays.asList(contextInitializers));
      return this;
   }

   /**
    * @deprecated since 11.0. To be removed in 14.0. Use {@link RemoteCacheConfigurationBuilder#nearCacheMode(NearCacheMode)} and {@link RemoteCacheConfigurationBuilder#nearCacheMaxEntries(int)} instead.
    */
   @Deprecated
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
   public ConfigurationBuilder uri(URI uri) {
      this.read(HotRodURI.create(uri).toConfigurationBuilder().build(false));
      return this;
   }

   @Override
   public ConfigurationBuilder uri(String uri) {
      this.read(HotRodURI.create(uri).toConfigurationBuilder().build(false));
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
   public StatisticsConfigurationBuilder statistics() {
      return statistics;
   }

   @Override
   public TransactionConfigurationBuilder transaction() {
      return transaction;
   }

   @Override
   public RemoteCacheConfigurationBuilder remoteCache(String name) {
      return remoteCacheBuilders.computeIfAbsent(name, (n) -> new RemoteCacheConfigurationBuilder(this, n));
   }

   @Override
   public ConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);

      if (typed.containsKey(ConfigurationProperties.URI)) {
         HotRodURI uri = HotRodURI.create(typed.getProperty(ConfigurationProperties.URI));
         this.read(uri.toConfigurationBuilder().build());
      }

      if (typed.containsKey(ConfigurationProperties.ASYNC_EXECUTOR_FACTORY)) {
         this.asyncExecutorFactory().factoryClass(typed.getProperty(ConfigurationProperties.ASYNC_EXECUTOR_FACTORY, null, true));
      }
      this.asyncExecutorFactory().withExecutorProperties(typed);
      String balancingStrategyClass = typed.getProperty(ConfigurationProperties.REQUEST_BALANCING_STRATEGY, null, true);
      if (balancingStrategyClass != null) {
         this.balancingStrategy(balancingStrategyClass);
      }
      this.clientIntelligence(typed.getEnumProperty(ConfigurationProperties.CLIENT_INTELLIGENCE, ClientIntelligence.class, ClientIntelligence.getDefault(), true));
      this.connectionPool.withPoolProperties(typed);
      this.connectionTimeout(typed.getIntProperty(ConfigurationProperties.CONNECT_TIMEOUT, connectionTimeout, true));
      if (typed.containsKey(ConfigurationProperties.HASH_FUNCTION_PREFIX + ".1")) {
         log.warn("Hash function version 1 is no longer supported");
      }
      for (int i = 0; i < consistentHashImpl.length; i++) {
         if (consistentHashImpl[i] != null) {
            int version = i + 1;
            String hashClassName = typed.getProperty(ConfigurationProperties.HASH_FUNCTION_PREFIX + "." + version,
                  null, true);
            if (hashClassName != null) {
               this.consistentHashImpl(version, hashClassName);
            }
         }
      }
      this.forceReturnValues(typed.getBooleanProperty(ConfigurationProperties.FORCE_RETURN_VALUES, forceReturnValues, true));
      this.keySizeEstimate(typed.getIntProperty(ConfigurationProperties.KEY_SIZE_ESTIMATE, keySizeEstimate, true));
      if (typed.containsKey(ConfigurationProperties.MARSHALLER)) {
         this.marshaller(typed.getProperty(ConfigurationProperties.MARSHALLER, null, true));
      }
      if (typed.containsKey(ConfigurationProperties.CONTEXT_INITIALIZERS)) {
         String initializers = typed.getProperty(ConfigurationProperties.CONTEXT_INITIALIZERS);
         for (String sci : initializers.split(","))
            this.addContextInitializer(sci);
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
      this.valueSizeEstimate(typed.getIntProperty(ConfigurationProperties.VALUE_SIZE_ESTIMATE, valueSizeEstimate, true));
      this.maxRetries(typed.getIntProperty(ConfigurationProperties.MAX_RETRIES, maxRetries, true));
      this.security.ssl().withProperties(properties);
      this.security.authentication().withProperties(properties);

      String serialWhitelist = typed.getProperty(ConfigurationProperties.JAVA_SERIAL_WHITELIST);
      if (serialWhitelist != null && !serialWhitelist.isEmpty()) {
         String[] classes = serialWhitelist.split(",");
         Collections.addAll(this.whiteListRegExs, classes);
      }

      this.batchSize(typed.getIntProperty(ConfigurationProperties.BATCH_SIZE, batchSize, true));
      transaction.withTransactionProperties(properties);
      nearCache.withProperties(properties);

      Map<String, String> xsiteProperties = typed.entrySet().stream()
            .filter(e -> ((String) e.getKey()).startsWith(ConfigurationProperties.CLUSTER_PROPERTIES_PREFIX))
            .collect(Collectors.toMap(
                  e -> ConfigurationProperties.CLUSTER_PROPERTIES_PREFIX_REGEX
                        .matcher((String) e.getKey()).replaceFirst(""),
                  e -> StringPropertyReplacer.replaceProperties((String) e.getValue())));
      xsiteProperties.entrySet().forEach(entry -> {
         ClusterConfigurationBuilder cluster = this.addCluster(entry.getKey());
         parseServers(entry.getValue(), (host, port) -> cluster.addClusterNode(host, port));
      });

      Set<String> cachesNames = typed.keySet().stream()
            .map(k -> (String)k)
            .filter(k -> k.startsWith(ConfigurationProperties.CACHE_PREFIX))
            .map(k ->
                  k.charAt(CACHE_PREFIX_LENGTH) =='[' ?
                        k.substring(CACHE_PREFIX_LENGTH + 1, k.indexOf(']', CACHE_PREFIX_LENGTH)) :
                        k.substring(CACHE_PREFIX_LENGTH, k.indexOf('.', CACHE_PREFIX_LENGTH + 1 ))
            ).collect(Collectors.toSet());

      for(String cacheName : cachesNames) {
         this.remoteCache(cacheName).withProperties(typed);
      }

      statistics.withProperties(properties);
      return this;
   }

   @Override
   public void validate() {
      connectionPool.validate();
      asyncExecutorFactory.validate();
      security.validate();
      nearCache.validate();
      transaction.validate();
      statistics.validate();
      if (maxRetries < 0) {
         throw HOTROD.invalidMaxRetries(maxRetries);
      }
      Set<String> clusterNameSet = new HashSet<>(clusters.size());
      for (ClusterConfigurationBuilder clusterConfigBuilder : clusters) {
         if (!clusterNameSet.add(clusterConfigBuilder.getClusterName())) {
            throw HOTROD.duplicateClusterDefinition(clusterConfigBuilder.getClusterName());
         }
         clusterConfigBuilder.validate();
      }
   }

   @Override
   public Configuration create() {
      List<ServerConfiguration> servers = new ArrayList<>();
      if (this.servers.size() > 0)
         for (ServerConfigurationBuilder server : this.servers) {
            servers.add(server.create());
         }
      else {
         servers.add(new ServerConfiguration("127.0.0.1", ConfigurationProperties.DEFAULT_HOTROD_PORT));
      }

      List<ClusterConfiguration> serverClusterConfigs = clusters.stream()
            .map(ClusterConfigurationBuilder::create).collect(Collectors.toList());

      Marshaller buildMarshaller = this.marshaller;
      if (buildMarshaller == null && marshallerClass == null) {
         buildMarshaller = handleNullMarshaller();
      }
      Class<? extends Marshaller> buildMarshallerClass = this.marshallerClass;
      if (buildMarshallerClass == null) {
         // Populate the marshaller class as well, so it can be exported to properties
         buildMarshallerClass = buildMarshaller.getClass();
      } else {
         if (buildMarshaller != null && !buildMarshallerClass.isInstance(buildMarshaller))
            throw new IllegalArgumentException("Both marshaller and marshallerClass attributes are present, but marshaller is not an instance of marshallerClass");
      }

      Map<String, RemoteCacheConfiguration> remoteCaches = remoteCacheBuilders.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().create()));

      return new Configuration(asyncExecutorFactory.create(), balancingStrategyFactory, classLoader == null ? null : classLoader.get(), clientIntelligence, connectionPool.create(), connectionTimeout,
                               consistentHashImpl, forceReturnValues, keySizeEstimate, buildMarshaller, buildMarshallerClass, protocolVersion, servers, socketTimeout, security.create(), tcpNoDelay, tcpKeepAlive,
                               valueSizeEstimate, maxRetries, nearCache.create(), serverClusterConfigs, whiteListRegExs, batchSize, transaction.create(), statistics.create(), features, contextInitializers, remoteCaches);
   }

   // Method that handles default marshaller - needed as a placeholder
   private Marshaller handleNullMarshaller() {
      return new ProtoStreamMarshaller();
   }

   @Override
   public Configuration build() {
      features = new Features(classLoader.get());
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
      this.classLoader = new WeakReference<>(template.classLoader());
      this.asyncExecutorFactory.read(template.asyncExecutorFactory());
      this.balancingStrategyFactory = template.balancingStrategyFactory();
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
      this.clusters.clear();
      template.clusters().forEach(cluster -> this.addCluster(cluster.getClusterName()).read(cluster));
      this.socketTimeout = template.socketTimeout();
      this.security.read(template.security());
      this.tcpNoDelay = template.tcpNoDelay();
      this.tcpKeepAlive = template.tcpKeepAlive();
      this.valueSizeEstimate = template.valueSizeEstimate();
      this.maxRetries = template.maxRetries();
      this.nearCache.read(template.nearCache());
      this.whiteListRegExs.addAll(template.serialWhitelist());
      this.transaction.read(template.transaction());
      this.statistics.read(template.statistics());
      this.contextInitializers.clear();
      this.contextInitializers.addAll(template.getContextInitializers());

      return this;
   }
}

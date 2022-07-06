package org.infinispan.hotrod.configuration;

import static org.infinispan.hotrod.configuration.HotRodConfiguration.ALLOW_LIST;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.BATCH_SIZE;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.CLIENT_INTELLIGENCE;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.CONNECT_TIMEOUT;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.CONSISTENT_HASH_IMPL;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.FORCE_RETURN_VALUES;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.MARSHALLER;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.MARSHALLER_CLASS;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.MAX_RETRIES;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.SOCKET_TIMEOUT;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.TCP_KEEPALIVE;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.TCP_NODELAY;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.TRANSACTION_TIMEOUT;
import static org.infinispan.hotrod.configuration.HotRodConfiguration.VERSION;
import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.infinispan.api.configuration.Configuration;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Features;
import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.HotRod;
import org.infinispan.hotrod.impl.ConfigurationProperties;
import org.infinispan.hotrod.impl.HotRodURI;
import org.infinispan.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * <p>ConfigurationBuilder used to generate immutable {@link HotRodConfiguration} objects to pass to the
 * {@link org.infinispan.api.Infinispan#create(Configuration)} method.</p>
 *
 * <p>If you prefer to configure the client declaratively, see {@link org.infinispan.hotrod.configuration}</p>
 *
 * @since 14.0
 */
public class HotRodConfigurationBuilder implements ConfigurationChildBuilder, Builder<HotRodConfiguration> {

   private static final Log log = LogFactory.getLog(HotRodConfigurationBuilder.class, Log.class);
   private final AttributeSet attributes = HotRodConfiguration.attributeDefinitionSet();
   // Match IPv4 (host:port) or IPv6 ([host]:port) addresses
   private static final Pattern ADDRESS_PATTERN = Pattern
         .compile("(\\[([0-9A-Fa-f:]+)\\]|([^:/?#]*))(?::(\\d*))?");
   private static final int CACHE_PREFIX_LENGTH = ConfigurationProperties.CACHE_PREFIX.length();

   private final ExecutorFactoryConfigurationBuilder asyncExecutorFactory;
   private Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory = RoundRobinBalancingStrategy::new;
   private final ConnectionPoolConfigurationBuilder connectionPool;
   private final List<ServerConfigurationBuilder> servers = new ArrayList<>();
   private final SecurityConfigurationBuilder security;
   private final List<String> allowListRegExs = new ArrayList<>();
   private final StatisticsConfigurationBuilder statistics;
   private final List<ClusterConfigurationBuilder> clusters = new ArrayList<>();
   private Features features;
   private final List<SerializationContextInitializer> contextInitializers = new ArrayList<>();
   private final Map<String, RemoteCacheConfigurationBuilder> remoteCacheBuilders;
   private TransportFactory transportFactory = TransportFactory.DEFAULT;

   public HotRodConfigurationBuilder() {
      this.connectionPool = new ConnectionPoolConfigurationBuilder(this);
      this.asyncExecutorFactory = new ExecutorFactoryConfigurationBuilder(this);
      this.security = new SecurityConfigurationBuilder(this);
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
   public HotRodConfigurationBuilder addServers(String servers) {
      parseServers(servers, (host, port) -> addServer().host(host).port(port));
      return this;
   }

   public List<ServerConfigurationBuilder> servers() {
      return servers;
   }

   public static void parseServers(String servers, BiConsumer<String, Integer> c) {
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
   public HotRodConfigurationBuilder balancingStrategy(String balancingStrategy) {
      this.balancingStrategyFactory = () -> Util.getInstance(balancingStrategy, HotRod.class.getClassLoader());
      return this;
   }

   @Override
   public HotRodConfigurationBuilder balancingStrategy(Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory) {
      this.balancingStrategyFactory = balancingStrategyFactory;
      return this;
   }

   @Override
   public HotRodConfigurationBuilder balancingStrategy(Class<? extends FailoverRequestBalancingStrategy> balancingStrategy) {
      this.balancingStrategyFactory = () -> Util.getInstance(balancingStrategy);
      return this;
   }

   @Override
   public HotRodConfigurationBuilder clientIntelligence(ClientIntelligence clientIntelligence) {
      attributes.attribute(CLIENT_INTELLIGENCE).set(clientIntelligence);
      return this;
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return connectionPool;
   }

   @Override
   public HotRodConfigurationBuilder connectionTimeout(int connectionTimeout) {
      attributes.attribute(CONNECT_TIMEOUT).set(connectionTimeout);
      return this;
   }

   @Override
   public HotRodConfigurationBuilder consistentHashImpl(int version, Class<? extends ConsistentHash> consistentHashClass) {
      if (version == 1) {
         log.warn("Hash function version 1 is no longer supported.");
      } else {
         attributes.attribute(CONSISTENT_HASH_IMPL).get()[version - 1] = consistentHashClass;
      }
      return this;
   }

   @Override
   public HotRodConfigurationBuilder consistentHashImpl(int version, String consistentHashClass) {
      return consistentHashImpl(version, Util.loadClass(consistentHashClass, HotRod.class.getClassLoader()));
   }

   @Override
   public HotRodConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      attributes.attribute(FORCE_RETURN_VALUES).set(forceReturnValues);
      return this;
   }

   @Override
   public HotRodConfigurationBuilder marshaller(String marshallerClassName) {
      return marshaller(marshallerClassName == null ? null : Util.loadClass(marshallerClassName, HotRod.class.getClassLoader()));
   }

   @Override
   public HotRodConfigurationBuilder marshaller(Class<? extends Marshaller> marshallerClass) {
      attributes.attribute(MARSHALLER).set(marshallerClass == null ? null : Util.getInstance(marshallerClass));
      attributes.attribute(MARSHALLER_CLASS).set(marshallerClass);
      return this;
   }

   @Override
   public HotRodConfigurationBuilder marshaller(Marshaller marshaller) {
      attributes.attribute(MARSHALLER).set(marshaller);
      attributes.attribute(MARSHALLER_CLASS).set(marshaller == null ? null : marshaller.getClass());
      return this;
   }

   @Override
   public HotRodConfigurationBuilder addContextInitializer(String contextInitializer) {
      SerializationContextInitializer sci = Util.getInstance(contextInitializer, HotRod.class.getClassLoader());
      return addContextInitializers(sci);
   }

   @Override
   public HotRodConfigurationBuilder addContextInitializer(SerializationContextInitializer contextInitializer) {
      if (contextInitializer != null)
         this.contextInitializers.add(contextInitializer);
      return this;
   }

   @Override
   public HotRodConfigurationBuilder addContextInitializers(SerializationContextInitializer... contextInitializers) {
      this.contextInitializers.addAll(Arrays.asList(contextInitializers));
      return this;
   }

   @Override
   public HotRodConfigurationBuilder version(ProtocolVersion protocolVersion) {
      attributes.attribute(VERSION).set(protocolVersion);
      return this;
   }

   @Override
   public SecurityConfigurationBuilder security() {
      return security;
   }

   @Override
   public HotRodConfigurationBuilder socketTimeout(int socketTimeout) {
      attributes.attribute(SOCKET_TIMEOUT).set(socketTimeout);
      return this;
   }

   @Override
   public HotRodConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      attributes.attribute(TCP_NODELAY).set(tcpNoDelay);
      return this;
   }

   @Override
   public HotRodConfigurationBuilder tcpKeepAlive(boolean keepAlive) {
      attributes.attribute(TCP_KEEPALIVE).set(keepAlive);
      return this;
   }

   @Override
   public HotRodConfigurationBuilder uri(URI uri) {
      // it returns this
      return HotRodURI.create(uri).toConfigurationBuilder(this);
   }

   @Override
   public HotRodConfigurationBuilder uri(String uri) {
      return uri(java.net.URI.create(uri));
   }

   @Override
   public HotRodConfigurationBuilder maxRetries(int maxRetries) {
      attributes.attribute(MAX_RETRIES).set(maxRetries);
      return this;
   }

   @Override
   public HotRodConfigurationBuilder addJavaSerialAllowList(String... regEx) {
      this.allowListRegExs.addAll(Arrays.asList(regEx));
      attributes.attribute(ALLOW_LIST).set(allowListRegExs.toArray(new String[0]));
      return this;
   }

   @Override
   public HotRodConfigurationBuilder batchSize(int batchSize) {
      if (batchSize <= 0) {
         throw new IllegalArgumentException("batchSize must be greater than 0");
      }
      attributes.attribute(BATCH_SIZE).set(batchSize);
      return this;
   }

   @Override
   public StatisticsConfigurationBuilder statistics() {
      return statistics;
   }

   @Override
   public RemoteCacheConfigurationBuilder remoteCache(String name) {
      return remoteCacheBuilders.computeIfAbsent(name, (n) -> new RemoteCacheConfigurationBuilder(this, n));
   }

   @Override
   public HotRodConfigurationBuilder transactionTimeout(long timeout, TimeUnit timeUnit) {
      attributes.attribute(TRANSACTION_TIMEOUT).set(timeUnit.toMillis(timeout));
      return this;
   }

   @Override
   public HotRodConfigurationBuilder transportFactory(TransportFactory transportFactory) {
      this.transportFactory = transportFactory;
      return this;
   }

   @Override
   public HotRodConfigurationBuilder withProperties(Properties properties) {
      //FIXME
      return this;
   }

   @Override
   public void validate() {
      attributes.validate();
      connectionPool.validate();
      asyncExecutorFactory.validate();
      security.validate();
      statistics.validate();
      if (attributes.attribute(MAX_RETRIES).get() < 0) {
         throw HOTROD.invalidMaxRetries(attributes.attribute(MAX_RETRIES).get());
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
   public HotRodConfiguration create() {
      List<ServerConfiguration> servers = new ArrayList<>();
      if (this.servers.size() > 0)
         for (ServerConfigurationBuilder server : this.servers) {
            servers.add(server.create());
         }
      else {
         servers.add(new ServerConfiguration(ServerConfiguration.attributeDefinitionSet().protect()));
      }

      List<ClusterConfiguration> serverClusterConfigs = clusters.stream()
            .map(ClusterConfigurationBuilder::create).collect(Collectors.toList());

      Map<String, RemoteCacheConfiguration> remoteCaches = remoteCacheBuilders.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey, e -> e.getValue().create()));

      return new HotRodConfiguration(attributes.protect(),
            asyncExecutorFactory.create(),
            balancingStrategyFactory,
            connectionPool.create(),
            servers,
            security.create(),
            serverClusterConfigs,
            statistics.create(),
            features,
            contextInitializers,
            remoteCaches,
            transportFactory);
   }

   // Method that handles default marshaller - needed as a placeholder
   private Marshaller handleNullMarshaller() {
      return new ProtoStreamMarshaller();
   }

   @Override
   public HotRodConfiguration build() {
      features = new Features(HotRod.class.getClassLoader());
      return build(true);
   }

   public HotRodConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public HotRodConfigurationBuilder read(HotRodConfiguration template) {
      this.attributes.read(template.attributes());
      this.asyncExecutorFactory.read(template.asyncExecutorFactory());
      this.balancingStrategyFactory = template.balancingStrategyFactory();
      this.connectionPool.read(template.connectionPool());
      this.servers.clear();
      for (ServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }
      this.clusters.clear();
      template.clusters().forEach(cluster -> this.addCluster(cluster.getClusterName()).read(cluster));
      this.security.read(template.security());
      this.transportFactory = template.transportFactory();
      this.statistics.read(template.statistics());
      this.contextInitializers.clear();
      this.contextInitializers.addAll(template.getContextInitializers());
      return this;
   }
}

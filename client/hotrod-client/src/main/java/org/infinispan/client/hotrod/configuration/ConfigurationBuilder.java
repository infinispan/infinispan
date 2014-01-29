package org.infinispan.client.hotrod.configuration;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.TypedProperties;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV2;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.RequestBalancingStrategy;
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
   private Class<? extends RequestBalancingStrategy> balancingStrategy = RoundRobinBalancingStrategy.class;
   private final ConnectionPoolConfigurationBuilder connectionPool;
   private int connectionTimeout = ConfigurationProperties.DEFAULT_CONNECT_TIMEOUT;
   @SuppressWarnings("unchecked")
   private Class<? extends ConsistentHash> consistentHashImpl[] = new Class[] { ConsistentHashV1.class, ConsistentHashV2.class };
   private boolean forceReturnValues;
   private int keySizeEstimate = ConfigurationProperties.DEFAULT_KEY_SIZE;
   private Class<? extends Marshaller> marshallerClass = GenericJBossMarshaller.class;
   private Marshaller marshaller;
   private boolean pingOnStartup = true;
   private String protocolVersion = ConfigurationProperties.DEFAULT_PROTOCOL_VERSION;
   private List<ServerConfigurationBuilder> servers = new ArrayList<ServerConfigurationBuilder>();
   private int socketTimeout = ConfigurationProperties.DEFAULT_SO_TIMEOUT;
   private final SslConfigurationBuilder ssl;
   private boolean tcpNoDelay = true;
   private Class<? extends TransportFactory> transportFactory = TcpTransportFactory.class;
   private int valueSizeEstimate = ConfigurationProperties.DEFAULT_VALUE_SIZE;
   private int maxRetries = ConfigurationProperties.DEFAULT_MAX_RETRIES;


   public ConfigurationBuilder() {
      this.classLoader = new WeakReference<ClassLoader>(Thread.currentThread().getContextClassLoader());
      this.connectionPool = new ConnectionPoolConfigurationBuilder(this);
      this.asyncExecutorFactory = new ExecutorFactoryConfigurationBuilder(this);
      this.ssl = new SslConfigurationBuilder(this);
   }

   @Override
   public ServerConfigurationBuilder addServer() {
      ServerConfigurationBuilder builder = new ServerConfigurationBuilder(this);
      this.servers.add(builder);
      return builder;
   }

   @Override
   public ConfigurationBuilder addServers(String servers) {
      for (String server : servers.split(";")) {
         Matcher matcher = ADDRESS_PATTERN.matcher(server);
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
      this.balancingStrategy = Util.loadClass(balancingStrategy, this.classLoader());
      return this;
   }

   @Override
   public ConfigurationBuilder balancingStrategy(Class<? extends RequestBalancingStrategy> balancingStrategy) {
      this.balancingStrategy = balancingStrategy;
      return this;
   }

   @Override
   public ConfigurationBuilder classLoader(ClassLoader cl) {
      this.classLoader = new WeakReference<ClassLoader>(cl);
      return this;
   }

   ClassLoader classLoader() {
      return classLoader != null ? classLoader.get() : null;
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
      this.consistentHashImpl[version - 1] = consistentHashClass;
      return this;
   }

   @Override
   public ConfigurationBuilder consistentHashImpl(int version, String consistentHashClass) {
      this.consistentHashImpl[version - 1] = Util.loadClass(consistentHashClass, classLoader());
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

   @Override
   public ConfigurationBuilder pingOnStartup(boolean pingOnStartup) {
      this.pingOnStartup = pingOnStartup;
      return this;
   }

   @Override
   public ConfigurationBuilder protocolVersion(String protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
   }

   @Override
   public ConfigurationBuilder socketTimeout(int socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
   }

   @Override
   public SslConfigurationBuilder ssl() {
      return ssl;
   }

   @Override
   public ConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      this.tcpNoDelay = tcpNoDelay;
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
   public ConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);

      if (typed.containsKey(ConfigurationProperties.ASYNC_EXECUTOR_FACTORY)) {
         this.asyncExecutorFactory().factoryClass(typed.getProperty(ConfigurationProperties.ASYNC_EXECUTOR_FACTORY));
      }
      this.asyncExecutorFactory().withExecutorProperties(typed);
      this.balancingStrategy(typed.getProperty(ConfigurationProperties.REQUEST_BALANCING_STRATEGY, balancingStrategy.getName()));
      this.connectionPool.withPoolProperties(typed);
      this.connectionTimeout(typed.getIntProperty(ConfigurationProperties.CONNECT_TIMEOUT, connectionTimeout));
      for (int i = 1; i <= consistentHashImpl.length; i++) {
         this.consistentHashImpl(i, typed.getProperty(ConfigurationProperties.HASH_FUNCTION_PREFIX + "." + i, consistentHashImpl[i - 1].getName()));
      }
      this.forceReturnValues(typed.getBooleanProperty(ConfigurationProperties.FORCE_RETURN_VALUES, forceReturnValues));
      this.keySizeEstimate(typed.getIntProperty(ConfigurationProperties.KEY_SIZE_ESTIMATE, keySizeEstimate));
      if (typed.containsKey(ConfigurationProperties.MARSHALLER)) {
         this.marshaller(typed.getProperty(ConfigurationProperties.MARSHALLER));
      }
      this.pingOnStartup(typed.getBooleanProperty(ConfigurationProperties.PING_ON_STARTUP, pingOnStartup));
      this.protocolVersion(typed.getProperty(ConfigurationProperties.PROTOCOL_VERSION, protocolVersion));
      this.servers.clear();
      this.addServers(typed.getProperty(ConfigurationProperties.SERVER_LIST, ""));
      this.socketTimeout(typed.getIntProperty(ConfigurationProperties.SO_TIMEOUT, socketTimeout));
      this.tcpNoDelay(typed.getBooleanProperty(ConfigurationProperties.TCP_NO_DELAY, tcpNoDelay));
      if (typed.containsKey(ConfigurationProperties.TRANSPORT_FACTORY)) {
         this.transportFactory(typed.getProperty(ConfigurationProperties.TRANSPORT_FACTORY));
      }
      this.valueSizeEstimate(typed.getIntProperty(ConfigurationProperties.VALUE_SIZE_ESTIMATE, valueSizeEstimate));
      this.maxRetries(typed.getIntProperty(ConfigurationProperties.MAX_RETRIES, maxRetries));
      return this;
   }

   @Override
   public void validate() {
      connectionPool.validate();
      asyncExecutorFactory.validate();
      ssl.validate();
      if (maxRetries < 0) {
         throw log.invalidMaxRetries(maxRetries);
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
      if (marshaller == null) {
         return new Configuration(asyncExecutorFactory.create(), balancingStrategy, classLoader == null ? null : classLoader.get(), connectionPool.create(), connectionTimeout,
               consistentHashImpl, forceReturnValues, keySizeEstimate, marshallerClass, pingOnStartup, protocolVersion, servers, socketTimeout, ssl.create(), tcpNoDelay, transportFactory,
               valueSizeEstimate, maxRetries);
      } else {
         return new Configuration(asyncExecutorFactory.create(), balancingStrategy, classLoader == null ? null : classLoader.get(), connectionPool.create(), connectionTimeout,
               consistentHashImpl, forceReturnValues, keySizeEstimate, marshaller, pingOnStartup, protocolVersion, servers, socketTimeout, ssl.create(), tcpNoDelay, transportFactory,
               valueSizeEstimate, maxRetries);
      }
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
      this.balancingStrategy = template.balancingStrategy();
      this.connectionPool.read(template.connectionPool());
      this.connectionTimeout = template.connectionTimeout();
      for (int i = 0; i < consistentHashImpl.length; i++) {
         this.consistentHashImpl[i] = template.consistentHashImpl()[i];
      }
      this.forceReturnValues = template.forceReturnValues();
      this.keySizeEstimate = template.keySizeEstimate();
      this.marshaller = template.marshaller();
      this.marshallerClass = template.marshallerClass();
      this.pingOnStartup = template.pingOnStartup();
      this.protocolVersion = template.protocolVersion();
      this.servers.clear();
      for (ServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }
      this.socketTimeout = template.socketTimeout();
      this.ssl.read(template.ssl());
      this.tcpNoDelay = template.tcpNoDelay();
      this.transportFactory = template.transportFactory();
      this.valueSizeEstimate = template.valueSizeEstimate();
      this.maxRetries = template.maxRetries();
      return this;
   }
}

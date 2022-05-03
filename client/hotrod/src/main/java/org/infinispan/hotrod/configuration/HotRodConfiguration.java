package org.infinispan.hotrod.configuration;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.api.configuration.Configuration;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Features;
import org.infinispan.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * Configuration.
 *
 * @since 14.0
 */
public class HotRodConfiguration extends ConfigurationElement<HotRodConfiguration> implements Configuration {
   static final AttributeDefinition<String[]> ALLOW_LIST = AttributeDefinition.builder("allow_list", new String[0], String[].class).immutable().build();
   static final AttributeDefinition<Integer> BATCH_SIZE = AttributeDefinition.builder("batch_size", 10_000, Integer.class).build();
   static final AttributeDefinition<ClientIntelligence> CLIENT_INTELLIGENCE = AttributeDefinition.builder("client_intelligence", ClientIntelligence.getDefault(), ClientIntelligence.class).immutable().build();
   static final AttributeDefinition<Integer> CONNECT_TIMEOUT = AttributeDefinition.builder("connect_timeout", 10_000, Integer.class).build();
   static final AttributeDefinition<Class[]> CONSISTENT_HASH_IMPL = AttributeDefinition.builder("hash_function_impl", ConsistentHash.DEFAULT, Class[].class).immutable().build();
   static final AttributeDefinition<Boolean> FORCE_RETURN_VALUES = AttributeDefinition.builder("force_return_values", false, Boolean.class).build();
   static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder("marshaller", null, Marshaller.class).immutable().initializer(ProtoStreamMarshaller::new).build();
   static final AttributeDefinition<Class> MARSHALLER_CLASS = AttributeDefinition.builder("marshaller_class", ProtoStreamMarshaller.class, Class.class).immutable().build();
   static final AttributeDefinition<Integer> MAX_RETRIES = AttributeDefinition.builder("max_retries", 60_000, Integer.class).validator(v -> {
      if (v < 0) throw HOTROD.invalidMaxRetries(v);
   }).build();
   static final AttributeDefinition<Integer> SOCKET_TIMEOUT = AttributeDefinition.builder("socket_timeout", 60_000, Integer.class).build();
   static final AttributeDefinition<Boolean> TCP_KEEPALIVE = AttributeDefinition.builder("tcp_keepalive", true, Boolean.class).build();
   static final AttributeDefinition<Boolean> TCP_NODELAY = AttributeDefinition.builder("tcp_no_delay", true, Boolean.class).build();
   static final AttributeDefinition<Long> TRANSACTION_TIMEOUT = AttributeDefinition.builder("transaction_timeout", 60_000l, Long.class).build();
   static final AttributeDefinition<String> URI = AttributeDefinition.builder("uri", null, String.class).immutable().build();
   static final AttributeDefinition<ProtocolVersion> VERSION = AttributeDefinition.builder("version", ProtocolVersion.PROTOCOL_VERSION_AUTO, ProtocolVersion.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(HotRodConfiguration.class, ALLOW_LIST, BATCH_SIZE, CLIENT_INTELLIGENCE, CONNECT_TIMEOUT,
            CONSISTENT_HASH_IMPL, FORCE_RETURN_VALUES, MARSHALLER, MARSHALLER_CLASS, MAX_RETRIES, SOCKET_TIMEOUT,
            TCP_KEEPALIVE, TCP_NODELAY, TRANSACTION_TIMEOUT, URI, VERSION);
   }

   private final ExecutorFactoryConfiguration asyncExecutorFactory;
   private final Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory;
   private final ConnectionPoolConfiguration connectionPool;
   private final List<ServerConfiguration> servers;
   private final SecurityConfiguration security;
   private final List<ClusterConfiguration> clusters;
   private final ClassAllowList classAllowList;
   private final StatisticsConfiguration statistics;
   private final Features features;
   private final List<SerializationContextInitializer> contextInitializers;
   private final Map<String, RemoteCacheConfiguration> remoteCaches;
   private final TransportFactory transportFactory;

   HotRodConfiguration(AttributeSet attributes,
                       ExecutorFactoryConfiguration asyncExecutorFactory,
                       Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory,
                       ConnectionPoolConfiguration connectionPool,
                       List<ServerConfiguration> servers,
                       SecurityConfiguration security,
                       List<ClusterConfiguration> clusters,
                       StatisticsConfiguration statistics,
                       Features features,
                       List<SerializationContextInitializer> contextInitializers,
                       Map<String, RemoteCacheConfiguration> remoteCaches,
                       TransportFactory transportFactory) {
      super("", attributes);
      this.asyncExecutorFactory = asyncExecutorFactory;
      this.balancingStrategyFactory = balancingStrategyFactory;
      this.connectionPool = connectionPool;
      this.servers = Collections.unmodifiableList(servers);
      this.security = security;
      this.clusters = clusters;
      this.classAllowList = new ClassAllowList(Arrays.asList(attributes.attribute(ALLOW_LIST).get()));
      this.statistics = statistics;
      this.features = features;
      this.contextInitializers = contextInitializers;
      this.remoteCaches = remoteCaches;
      this.transportFactory = transportFactory;
   }

   public ExecutorFactoryConfiguration asyncExecutorFactory() {
      return asyncExecutorFactory;
   }

   public Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory() {
      return balancingStrategyFactory;
   }

   public ClientIntelligence clientIntelligence() {
      return attributes.attribute(CLIENT_INTELLIGENCE).get();
   }

   public ConnectionPoolConfiguration connectionPool() {
      return connectionPool;
   }

   public int connectionTimeout() {
      return attributes.attribute(CONNECT_TIMEOUT).get();
   }

   public Class<? extends ConsistentHash>[] consistentHashImpl() {
      Class[] classes = attributes.attribute(CONSISTENT_HASH_IMPL).get();
      return Arrays.copyOf(classes, classes.length);
   }

   public Class<? extends ConsistentHash> consistentHashImpl(int version) {
      return attributes.attribute(CONSISTENT_HASH_IMPL).get()[version - 1];
   }

   public boolean forceReturnValues() {
      return attributes.attribute(FORCE_RETURN_VALUES).get();
   }

   public Marshaller marshaller() {
      return attributes.attribute(MARSHALLER).get();
   }

   public Class<? extends Marshaller> marshallerClass() {
      return attributes.attribute(MARSHALLER_CLASS).get();
   }

   public ProtocolVersion version() {
      return attributes.attribute(VERSION).get();
   }

   public List<ServerConfiguration> servers() {
      return servers;
   }

   public List<ClusterConfiguration> clusters() {
      return clusters;
   }

   public int socketTimeout() {
      return attributes.attribute(SOCKET_TIMEOUT).get();
   }

   public SecurityConfiguration security() {
      return security;
   }

   public boolean tcpNoDelay() {
      return attributes.attribute(TCP_NODELAY).get();
   }

   public boolean tcpKeepAlive() {
      return attributes.attribute(TCP_KEEPALIVE).get();
   }

   public int maxRetries() {
      return attributes.attribute(MAX_RETRIES).get();
   }

   public String[] serialAllowList() {
      return attributes.attribute(ALLOW_LIST).get();
   }

   public ClassAllowList getClassAllowList() {
      return classAllowList;
   }

   public int batchSize() {
      return attributes.attribute(BATCH_SIZE).get();
   }

   public Map<String, RemoteCacheConfiguration> remoteCaches() {
      return Collections.unmodifiableMap(remoteCaches);
   }

   /**
    * Create a new {@link RemoteCacheConfiguration}. This can be used to create additional configurations after the
    * client has been initialized.
    *
    * @param name            the name of the cache configuration to create
    * @param builderConsumer a {@link Consumer} which receives a {@link RemoteCacheConfigurationBuilder} and can apply
    *                        the necessary configurations on it.
    * @return the {@link RemoteCacheConfiguration}
    * @throws IllegalArgumentException if a cache configuration with the same name already exists
    */
   public RemoteCacheConfiguration addRemoteCache(String name, Consumer<RemoteCacheConfigurationBuilder> builderConsumer) {
      synchronized (remoteCaches) {
         if (remoteCaches.containsKey(name)) {
            throw Log.HOTROD.duplicateCacheConfiguration(name);
         } else {
            RemoteCacheConfigurationBuilder builder = new RemoteCacheConfigurationBuilder(null, name);
            builderConsumer.accept(builder);
            builder.validate();
            RemoteCacheConfiguration configuration = builder.create();
            remoteCaches.put(name, configuration);
            return configuration;
         }
      }
   }

   /**
    * Remove a {@link RemoteCacheConfiguration} from this {@link HotRodConfiguration}. If the cache configuration
    * doesn't exist, this method has no effect.
    *
    * @param name the name of the {@link RemoteCacheConfiguration} to remove.
    */
   public void removeRemoteCache(String name) {
      remoteCaches.remove(name);
   }

   public StatisticsConfiguration statistics() {
      return statistics;
   }

   /**
    * see {@link HotRodConfigurationBuilder#transactionTimeout(long, TimeUnit)},
    */
   public long transactionTimeout() {
      return attributes.attribute(TRANSACTION_TIMEOUT).get();
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
}

package org.infinispan.persistence.remote.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeInitializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.remote.RemoteStore;

@BuiltBy(RemoteStoreConfigurationBuilder.class)
@ConfigurationFor(RemoteStore.class)
public class RemoteStoreConfiguration extends AbstractStoreConfiguration {
   static final AttributeDefinition<String> BALANCING_STRATEGY = AttributeDefinition.builder("balancingStrategy", RoundRobinBalancingStrategy.class.getName()).immutable().build();
   static final AttributeDefinition<Long> CONNECTION_TIMEOUT = AttributeDefinition.builder("connectionTimeout", (long)ConfigurationProperties.DEFAULT_CONNECT_TIMEOUT).build();
   static final AttributeDefinition<Boolean> FORCE_RETURN_VALUES = AttributeDefinition.builder("forceReturnValues", false).immutable().build();
   static final AttributeDefinition<Boolean> HOTROD_WRAPPING = AttributeDefinition.builder("hotRodWrapping", false).immutable().build();
   static final AttributeDefinition<Boolean> RAW_VALUES = AttributeDefinition.builder("rawValues", false).immutable().build();
   static final AttributeDefinition<Integer> KEY_SIZE_ESTIMATE = AttributeDefinition.builder("keySizeEstimate", ConfigurationProperties.DEFAULT_KEY_SIZE).immutable().build();
   static final AttributeDefinition<Integer> VALUE_SIZE_ESTIMATE = AttributeDefinition.builder("valueSizeEstimate", ConfigurationProperties.DEFAULT_VALUE_SIZE).immutable().build();
   static final AttributeDefinition<String> MARSHALLER = AttributeDefinition.builder("marshaller", null, String.class).immutable().build();
   static final AttributeDefinition<Boolean> PING_ON_STARTUP = AttributeDefinition.builder("pingOnStartup", true).immutable().build();
   static final AttributeDefinition<String> PROTOCOL_VERSION = AttributeDefinition.builder("protocolVersion", null, String.class).immutable().build();
   static final AttributeDefinition<String> REMOTE_CACHE_NAME = AttributeDefinition.builder("remoteCacheName", BasicCacheContainer.DEFAULT_CACHE_NAME).immutable().build();
   static final AttributeDefinition<List<RemoteServerConfiguration>> SERVERS = AttributeDefinition.builder("servers", null, (Class<List<RemoteServerConfiguration>>)(Class<?>)List.class).initializer(new AttributeInitializer<List<RemoteServerConfiguration>>() {
      @Override
      public List<RemoteServerConfiguration> initialize() {
         return new ArrayList<>();
      }
   }).build();
   static final AttributeDefinition<Long> SOCKET_TIMEOUT = AttributeDefinition.builder("socketTimeout", (long)ConfigurationProperties.DEFAULT_SO_TIMEOUT).build();
   static final AttributeDefinition<Boolean> TCP_NO_DELAY = AttributeDefinition.builder("tcpNoDelay", true).build();
   static final AttributeDefinition<String> TRANSPORT_FACTORY = AttributeDefinition.builder("transportFactory", null, String.class).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RemoteStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), BALANCING_STRATEGY, CONNECTION_TIMEOUT, FORCE_RETURN_VALUES,
            HOTROD_WRAPPING, RAW_VALUES, KEY_SIZE_ESTIMATE, MARSHALLER, PING_ON_STARTUP, PROTOCOL_VERSION, REMOTE_CACHE_NAME, SERVERS, SOCKET_TIMEOUT, TCP_NO_DELAY, TRANSPORT_FACTORY, VALUE_SIZE_ESTIMATE);
   }

   private final Attribute<String> balancingStrategy;
   private final Attribute<Long> connectionTimeout;
   private final Attribute<Boolean> forceReturnValues;
   private final Attribute<Boolean> hotRodWrapping;
   private final Attribute<Boolean> rawValues;
   private final Attribute<Integer> keySizeEstimate;
   private final Attribute<Integer> valueSizeEstimate;
   private final Attribute<String> marshaller;
   private final Attribute<Boolean> pingOnStartup;
   private final Attribute<String> protocolVersion;
   private final Attribute<String> remoteCacheName;
   private final Attribute<List<RemoteServerConfiguration>> servers;
   private final Attribute<Long> socketTimeout;
   private final Attribute<Boolean> tcpNoDelay;
   private final Attribute<String> transportFactory;
   private final ConnectionPoolConfiguration connectionPool;
   private final ExecutorFactoryConfiguration asyncExecutorFactory;

   public RemoteStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                   ExecutorFactoryConfiguration asyncExecutorFactory, ConnectionPoolConfiguration connectionPool) {
      super(attributes, async, singletonStore);
      balancingStrategy = attributes.attribute(BALANCING_STRATEGY);
      connectionTimeout = attributes.attribute(CONNECTION_TIMEOUT);
      forceReturnValues = attributes.attribute(FORCE_RETURN_VALUES);
      hotRodWrapping = attributes.attribute(HOTROD_WRAPPING);
      rawValues = attributes.attribute(RAW_VALUES);
      keySizeEstimate = attributes.attribute(KEY_SIZE_ESTIMATE);
      valueSizeEstimate = attributes.attribute(VALUE_SIZE_ESTIMATE);
      marshaller = attributes.attribute(MARSHALLER);
      pingOnStartup = attributes.attribute(PING_ON_STARTUP);
      protocolVersion = attributes.attribute(PROTOCOL_VERSION);
      remoteCacheName = attributes.attribute(REMOTE_CACHE_NAME);
      servers = attributes.attribute(SERVERS);
      socketTimeout = attributes.attribute(SOCKET_TIMEOUT);
      tcpNoDelay = attributes.attribute(TCP_NO_DELAY);
      transportFactory = attributes.attribute(TRANSPORT_FACTORY);
      this.asyncExecutorFactory = asyncExecutorFactory;
      this.connectionPool = connectionPool;
   }

   public ExecutorFactoryConfiguration asyncExecutorFactory() {
      return asyncExecutorFactory;
   }

   public String balancingStrategy() {
      return balancingStrategy.get();
   }

   public ConnectionPoolConfiguration connectionPool() {
      return connectionPool;
   }

   public long connectionTimeout() {
      return connectionTimeout.get();
   }

   public boolean forceReturnValues() {
      return forceReturnValues.get();
   }

   public boolean hotRodWrapping() {
      return hotRodWrapping.get();
   }

   public int keySizeEstimate() {
      return keySizeEstimate.get();
   }

   public String marshaller() {
      return marshaller.get();
   }

   public boolean pingOnStartup() {
      return pingOnStartup.get();
   }

   public String protocolVersion() {
      return protocolVersion.get();
   }

   public boolean rawValues() {
      return rawValues.get();
   }

   public String remoteCacheName() {
      return remoteCacheName.get();
   }

   public List<RemoteServerConfiguration> servers() {
      return servers.get();
   }

   public long socketTimeout() {
      return socketTimeout.get();
   }

   public boolean tcpNoDelay() {
      return tcpNoDelay.get();
   }

   public String transportFactory() {
      return transportFactory.get();
   }

   public int valueSizeEstimate() {
      return valueSizeEstimate.get();
   }
}


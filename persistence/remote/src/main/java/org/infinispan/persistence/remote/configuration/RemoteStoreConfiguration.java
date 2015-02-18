package org.infinispan.persistence.remote.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
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
   public static AttributeSet attributeSet() {
      return new AttributeSet(RemoteStoreConfiguration.class, AbstractStoreConfiguration.attributeSet(), BALANCING_STRATEGY, CONNECTION_TIMEOUT, FORCE_RETURN_VALUES,
            HOTROD_WRAPPING, RAW_VALUES, KEY_SIZE_ESTIMATE, MARSHALLER, PING_ON_STARTUP, PROTOCOL_VERSION, REMOTE_CACHE_NAME, SERVERS, SOCKET_TIMEOUT, TCP_NO_DELAY, TRANSPORT_FACTORY, VALUE_SIZE_ESTIMATE);
   }
   private final ConnectionPoolConfiguration connectionPool;
   private final ExecutorFactoryConfiguration asyncExecutorFactory;

   public RemoteStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                   ExecutorFactoryConfiguration asyncExecutorFactory, ConnectionPoolConfiguration connectionPool) {
      super(attributes, async, singletonStore);
      this.asyncExecutorFactory = asyncExecutorFactory;
      this.connectionPool = connectionPool;
   }

   public ExecutorFactoryConfiguration asyncExecutorFactory() {
      return asyncExecutorFactory;
   }

   public String balancingStrategy() {
      return attributes.attribute(BALANCING_STRATEGY).asString();
   }

   public ConnectionPoolConfiguration connectionPool() {
      return connectionPool;
   }

   public long connectionTimeout() {
      return attributes.attribute(CONNECTION_TIMEOUT).asLong();
   }

   public boolean forceReturnValues() {
      return attributes.attribute(FORCE_RETURN_VALUES).asBoolean();
   }

   public boolean hotRodWrapping() {
      return attributes.attribute(HOTROD_WRAPPING).asBoolean();
   }

   public int keySizeEstimate() {
      return attributes.attribute(KEY_SIZE_ESTIMATE).asInteger();
   }

   public String marshaller() {
      return attributes.attribute(MARSHALLER).asString();
   }

   public boolean pingOnStartup() {
      return attributes.attribute(PING_ON_STARTUP).asBoolean();
   }

   public String protocolVersion() {
      return attributes.attribute(PROTOCOL_VERSION).asString();
   }

   public boolean rawValues() {
      return attributes.attribute(RAW_VALUES).asBoolean();
   }

   public String remoteCacheName() {
      return attributes.attribute(REMOTE_CACHE_NAME).asString();
   }

   public List<RemoteServerConfiguration> servers() {
      return attributes.attribute(SERVERS).asObject(List.class);
   }

   public long socketTimeout() {
      return attributes.attribute(SOCKET_TIMEOUT).asLong();
   }

   public boolean tcpNoDelay() {
      return attributes.attribute(TCP_NO_DELAY).asBoolean();
   }

   public String transportFactory() {
      return attributes.attribute(TRANSPORT_FACTORY).asString();
   }

   public int valueSizeEstimate() {
      return attributes.attribute(VALUE_SIZE_ESTIMATE).asInteger();
   }
}


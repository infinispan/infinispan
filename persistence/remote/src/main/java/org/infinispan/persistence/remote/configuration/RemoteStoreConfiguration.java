package org.infinispan.persistence.remote.configuration;

import java.util.List;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.persistence.remote.RemoteStore;

@BuiltBy(RemoteStoreConfigurationBuilder.class)
@ConfigurationFor(RemoteStore.class)
@SerializedWith(RemoteStoreConfigurationSerializer.class)
public class RemoteStoreConfiguration extends AbstractStoreConfiguration<RemoteStoreConfiguration> {
   static final AttributeDefinition<String> BALANCING_STRATEGY = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.BALANCING_STRATEGY, RoundRobinBalancingStrategy.class.getName()).immutable().build();
   static final AttributeDefinition<Long> CONNECTION_TIMEOUT = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.CONNECT_TIMEOUT, (long) ConfigurationProperties.DEFAULT_CONNECT_TIMEOUT).build();
   static final AttributeDefinition<Boolean> FORCE_RETURN_VALUES = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.FORCE_RETURN_VALUES, false).immutable().build();
   static final AttributeDefinition<Boolean> HOTROD_WRAPPING = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.HOTROD_WRAPPING, false).immutable().build();
   static final AttributeDefinition<Boolean> RAW_VALUES = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.RAW_VALUES, false).immutable().build();
   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true)
   static final AttributeDefinition<Integer> KEY_SIZE_ESTIMATE = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.KEY_SIZE_ESTIMATE, ConfigurationProperties.DEFAULT_KEY_SIZE).immutable().build();
   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true)
   static final AttributeDefinition<Integer> VALUE_SIZE_ESTIMATE = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.VALUE_SIZE_ESTIMATE, ConfigurationProperties.DEFAULT_VALUE_SIZE).immutable().build();
   static final AttributeDefinition<String> MARSHALLER = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.MARSHALLER, null, String.class).immutable().build();
   static final AttributeDefinition<ProtocolVersion> PROTOCOL_VERSION = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.PROTOCOL_VERSION, ProtocolVersion.DEFAULT_PROTOCOL_VERSION)
         .immutable().build();
   static final AttributeDefinition<String> REMOTE_CACHE_CONTAINER = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.REMOTE_CACHE_CONTAINER, "").immutable().build();
   static final AttributeDefinition<String> REMOTE_CACHE_NAME = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.REMOTE_CACHE_NAME, "").immutable().build();

   static final AttributeDefinition<String> URI = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.URI, null, String.class).immutable()
         .build();

   static final AttributeDefinition<Long> SOCKET_TIMEOUT = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.SOCKET_TIMEOUT, (long) ConfigurationProperties.DEFAULT_SO_TIMEOUT).build();
   static final AttributeDefinition<Boolean> TCP_NO_DELAY = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.TCP_NO_DELAY, true).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RemoteStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), BALANCING_STRATEGY, CONNECTION_TIMEOUT, FORCE_RETURN_VALUES,
            HOTROD_WRAPPING, RAW_VALUES, KEY_SIZE_ESTIMATE, MARSHALLER, PROTOCOL_VERSION, REMOTE_CACHE_CONTAINER, REMOTE_CACHE_NAME, SOCKET_TIMEOUT, TCP_NO_DELAY, VALUE_SIZE_ESTIMATE, URI);
   }

   private final ConnectionPoolConfiguration connectionPool;
   private final ExecutorFactoryConfiguration asyncExecutorFactory;
   private final SecurityConfiguration security;
   private final List<RemoteServerConfiguration> servers;

   public RemoteStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
                                   ExecutorFactoryConfiguration asyncExecutorFactory, ConnectionPoolConfiguration connectionPool,
                                   SecurityConfiguration security, List<RemoteServerConfiguration> servers) {
      super(Element.REMOTE_STORE, attributes, async);
      this.asyncExecutorFactory = asyncExecutorFactory;
      this.connectionPool = connectionPool;
      this.security = security;
      this.servers = servers;
   }

   public String uri() {
      return attributes.attribute(URI).get();
   }

   public ExecutorFactoryConfiguration asyncExecutorFactory() {
      return asyncExecutorFactory;
   }

   public String balancingStrategy() {
      return attributes.attribute(BALANCING_STRATEGY).get();
   }

   public ConnectionPoolConfiguration connectionPool() {
      return connectionPool;
   }

   public long connectionTimeout() {
      return attributes.attribute(CONNECTION_TIMEOUT).get();
   }

   public boolean forceReturnValues() {
      return attributes.attribute(FORCE_RETURN_VALUES).get();
   }

   /**
    * @deprecated since 12.0 - Automatic media type detection in remote store makes this option redundant
    */
   @Deprecated(forRemoval=true)
   public boolean hotRodWrapping() {
      return attributes.attribute(RAW_VALUES).get();
   }

   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true)
   public int keySizeEstimate() {
      return attributes.attribute(KEY_SIZE_ESTIMATE).get();
   }

   public String marshaller() {
      return attributes.attribute(MARSHALLER).get();
   }

   public ProtocolVersion protocol() {
      return attributes.attribute(PROTOCOL_VERSION).get();
   }

   /**
    * @deprecated since 12.0 - This option can still be needed when retrieving from a preexisting remote cache
    */
   @Deprecated(forRemoval=true)
   public boolean rawValues() {
      return attributes.attribute(RAW_VALUES).get();
   }

   public String remoteCacheContainer() {
      return attributes.attribute(REMOTE_CACHE_CONTAINER).get();
   }

   public String remoteCacheName() {
      return attributes.attribute(REMOTE_CACHE_NAME).get();
   }

   public List<RemoteServerConfiguration> servers() {
      return servers;
   }

   public long socketTimeout() {
     return attributes.attribute(SOCKET_TIMEOUT).get();
   }

   public boolean tcpNoDelay() {
      return attributes.attribute(TCP_NO_DELAY).get();
   }

   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true)
   public int valueSizeEstimate() {
      return attributes.attribute(VALUE_SIZE_ESTIMATE).get();
   }

   public SecurityConfiguration security() {
      return security;
   }
}

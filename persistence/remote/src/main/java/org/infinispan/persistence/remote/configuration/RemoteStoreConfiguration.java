package org.infinispan.persistence.remote.configuration;

import java.util.List;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
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
   @Deprecated
   static final AttributeDefinition<Integer> KEY_SIZE_ESTIMATE = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.KEY_SIZE_ESTIMATE, ConfigurationProperties.DEFAULT_KEY_SIZE).immutable().build();
   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated
   static final AttributeDefinition<Integer> VALUE_SIZE_ESTIMATE = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.VALUE_SIZE_ESTIMATE, ConfigurationProperties.DEFAULT_VALUE_SIZE).immutable().build();
   static final AttributeDefinition<String> MARSHALLER = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.MARSHALLER, null, String.class).immutable().build();
   static final AttributeDefinition<ProtocolVersion> PROTOCOL_VERSION = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.PROTOCOL_VERSION, ProtocolVersion.DEFAULT_PROTOCOL_VERSION)
         .immutable().build();
   static final AttributeDefinition<String> REMOTE_CACHE_NAME = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.REMOTE_CACHE_NAME, "").immutable().build();

   static final AttributeDefinition<String> URI = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.URI, null, String.class).immutable()
         .build();

   static final AttributeDefinition<Long> SOCKET_TIMEOUT = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.SOCKET_TIMEOUT, (long) ConfigurationProperties.DEFAULT_SO_TIMEOUT).build();
   static final AttributeDefinition<Boolean> TCP_NO_DELAY = AttributeDefinition.builder(org.infinispan.persistence.remote.configuration.Attribute.TCP_NO_DELAY, true).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RemoteStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), BALANCING_STRATEGY, CONNECTION_TIMEOUT, FORCE_RETURN_VALUES,
            HOTROD_WRAPPING, RAW_VALUES, KEY_SIZE_ESTIMATE, MARSHALLER, PROTOCOL_VERSION, REMOTE_CACHE_NAME, SOCKET_TIMEOUT, TCP_NO_DELAY, VALUE_SIZE_ESTIMATE, URI);
   }

   private final Attribute<String> balancingStrategy;
   private final Attribute<Long> connectionTimeout;
   private final Attribute<Boolean> forceReturnValues;
   private final Attribute<Boolean> hotRodWrapping;
   private final Attribute<Boolean> rawValues;
   private final Attribute<Integer> keySizeEstimate;
   private final Attribute<Integer> valueSizeEstimate;
   private final Attribute<String> marshaller;
   private final Attribute<ProtocolVersion> protocolVersion;
   private final Attribute<String> remoteCacheName;
   private final Attribute<String> uri;
   private final Attribute<Long> socketTimeout;
   private final Attribute<Boolean> tcpNoDelay;
   private final ConnectionPoolConfiguration connectionPool;
   private final ExecutorFactoryConfiguration asyncExecutorFactory;
   private final SecurityConfiguration security;
   private List<RemoteServerConfiguration> servers;

   public RemoteStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
                                   ExecutorFactoryConfiguration asyncExecutorFactory, ConnectionPoolConfiguration connectionPool,
                                   SecurityConfiguration security, List<RemoteServerConfiguration> servers) {
      super(Element.REMOTE_STORE, attributes, async);
      balancingStrategy = attributes.attribute(BALANCING_STRATEGY);
      connectionTimeout = attributes.attribute(CONNECTION_TIMEOUT);
      forceReturnValues = attributes.attribute(FORCE_RETURN_VALUES);
      hotRodWrapping = attributes.attribute(HOTROD_WRAPPING);
      rawValues = attributes.attribute(RAW_VALUES);
      keySizeEstimate = attributes.attribute(KEY_SIZE_ESTIMATE);
      valueSizeEstimate = attributes.attribute(VALUE_SIZE_ESTIMATE);
      marshaller = attributes.attribute(MARSHALLER);
      protocolVersion = attributes.attribute(PROTOCOL_VERSION);
      remoteCacheName = attributes.attribute(REMOTE_CACHE_NAME);
      uri = attributes.attribute(URI);
      socketTimeout = attributes.attribute(SOCKET_TIMEOUT);
      tcpNoDelay = attributes.attribute(TCP_NO_DELAY);
      this.asyncExecutorFactory = asyncExecutorFactory;
      this.connectionPool = connectionPool;
      this.security = security;
      this.servers = servers;
   }

   public String uri() {
      return uri.get();
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

   /**
    * @deprecated since 12.0 - Automatic media type detection in remote store makes this option redundant
    */
   @Deprecated
   public boolean hotRodWrapping() {
      return hotRodWrapping.get();
   }

   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated
   public int keySizeEstimate() {
      return keySizeEstimate.get();
   }

   public String marshaller() {
      return marshaller.get();
   }

   public ProtocolVersion protocol() {
      return protocolVersion.get();
   }

   /**
    * @deprecated since 12.0 - This option can still be needed when retrieving from a preexisting remote cache
    */
   @Deprecated
   public boolean rawValues() {
      return rawValues.get();
   }

   public String remoteCacheName() {
      return remoteCacheName.get();
   }

   public List<RemoteServerConfiguration> servers() {
      return servers;
   }

   public long socketTimeout() {
      return socketTimeout.get();
   }

   public boolean tcpNoDelay() {
      return tcpNoDelay.get();
   }

   /**
    * @deprecated since 10.0. This method always returns null
    */
   @Deprecated
   public String transportFactory() {
      return null;
   }

   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated
   public int valueSizeEstimate() {
      return valueSizeEstimate.get();
   }

   public SecurityConfiguration security() {
      return security;
   }
}

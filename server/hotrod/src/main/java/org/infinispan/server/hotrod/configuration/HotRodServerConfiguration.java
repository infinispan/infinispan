package org.infinispan.server.hotrod.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.configuration.EncryptionConfiguration;
import org.infinispan.server.core.configuration.IpFilterConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SaslAuthenticationConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.hotrod.HotRodServer;

@BuiltBy(HotRodServerConfigurationBuilder.class)
@ConfigurationFor(HotRodServer.class)
public class HotRodServerConfiguration extends ProtocolServerConfiguration<HotRodServerConfiguration, SaslAuthenticationConfiguration> {
   public static final String TOPOLOGY_CACHE_NAME_PREFIX = "___hotRodTopologyCache";

   public static final AttributeDefinition<String> PROXY_HOST = AttributeDefinition.builder(Attribute.EXTERNAL_HOST, null, String.class).immutable().build();
   public static final AttributeDefinition<Integer> PROXY_PORT = AttributeDefinition.builder(Attribute.EXTERNAL_PORT, -1).immutable().build();
   public static final AttributeDefinition<Integer> LISTENER_BACKPRESSURE_HIGH_WATERMARK = AttributeDefinition.builder(Attribute.LISTENER_BACKPRESSURE_HIGH_WATERMARK, 100).build();
   public static final AttributeDefinition<Integer> LISTENER_BACKPRESSURE_LOW_WATERMARK = AttributeDefinition.builder(Attribute.LISTENER_BACKPRESSURE_LOW_WATERMARK, 50).build();
   public static final AttributeDefinition<Integer> LISTENER_MAX_QUEUE_SIZE = AttributeDefinition.builder(Attribute.LISTENER_MAX_QUEUE_SIZE, 512).build();

   private final TopologyCacheConfiguration topologyCache;
   private final EncryptionConfiguration encryption;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(HotRodServerConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet(),
            PROXY_HOST, PROXY_PORT, LISTENER_BACKPRESSURE_HIGH_WATERMARK, LISTENER_BACKPRESSURE_LOW_WATERMARK, LISTENER_MAX_QUEUE_SIZE);
   }

   HotRodServerConfiguration(AttributeSet attributes,
                             TopologyCacheConfiguration topologyCache,
                             SslConfiguration ssl, SaslAuthenticationConfiguration authentication,
                             EncryptionConfiguration encryption, IpFilterConfiguration ipRules) {
      super(Element.HOTROD_CONNECTOR, attributes, authentication, ssl, ipRules);
      this.topologyCache = topologyCache;
      this.encryption = encryption;
   }

   public String proxyHost() {
      return attributes.attribute(PROXY_HOST).get();
   }

   public String publicHost() {
      return attributes.attribute(PROXY_HOST).orElse(host());
   }

   public int proxyPort() {
      return attributes.attribute(PROXY_PORT).get();
   }

   public int publicPort() {
      return attributes.attribute(PROXY_PORT).orElse(port());
   }

   public String topologyCacheName() {
      String name = name();
      return TOPOLOGY_CACHE_NAME_PREFIX + (!name.isEmpty() ? "_" + name : name);
   }

   public long topologyLockTimeout() {
      return topologyCache.lockTimeout();
   }

   public long topologyReplTimeout() {
      return topologyCache.replicationTimeout();
   }

   public boolean topologyAwaitInitialTransfer() {
      return topologyCache.awaitInitialTransfer();
   }

   public boolean networkPrefixOverride() {
      return topologyCache.networkPrefixOverride();
   }

   public int listenerBackpressureHighWatermark() {
      return attributes.attribute(LISTENER_BACKPRESSURE_HIGH_WATERMARK).get();
   }

   public int listenerBackpressureLowWatermark() {
      return attributes.attribute(LISTENER_BACKPRESSURE_LOW_WATERMARK).get();
   }

   public int listenerMaxQueueSize() {
      return attributes.attribute(LISTENER_MAX_QUEUE_SIZE).get();
   }

   @Override
   public SaslAuthenticationConfiguration authentication() {
      return authentication;
   }

   public TopologyCacheConfiguration topologyCache() {
      return topologyCache;
   }

   public EncryptionConfiguration encryption() {
      return encryption;
   }
}

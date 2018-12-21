package org.infinispan.server.hotrod.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.hotrod.HotRodServer;

@BuiltBy(HotRodServerConfigurationBuilder.class)
@ConfigurationFor(HotRodServer.class)
public class HotRodServerConfiguration extends ProtocolServerConfiguration {
   public static final String TOPOLOGY_CACHE_NAME_PREFIX = "___hotRodTopologyCache";

   public static final AttributeDefinition<String> PROXY_HOST = AttributeDefinition.builder("proxy-host", null, String.class).immutable().build();
   public static final AttributeDefinition<Integer> PROXY_PORT = AttributeDefinition.builder("proxy-port", -1).immutable().build();
   public static final AttributeDefinition<Boolean> TOPOLOGY_AWAIT_INITIAL_TRANSFER = AttributeDefinition.builder("topology-await-initial-transfer", true).immutable().build();
   public static final AttributeDefinition<Long> TOPOLOGY_LOCK_TIMEOUT = AttributeDefinition.builder("topology-lock-timeout", 10000L).immutable().build();
   public static final AttributeDefinition<Long> TOPOLOGY_REPL_TIMEOUT = AttributeDefinition.builder("topology-repl-timeout", 10000L).immutable().build();
   public static final AttributeDefinition<Boolean> TOPOLOGY_STATE_TRANSFER = AttributeDefinition.builder("topology-state-transfer", true).immutable().build();

   private final Attribute<String> proxyHost;
   private final Attribute<Integer> proxyPort;
   private final Attribute<Long> topologyLockTimeout;
   private final Attribute<Long> topologyReplTimeout;
   private final Attribute<Boolean> topologyAwaitInitialTransfer;
   private final Attribute<Boolean> topologyStateTransfer;
   private final AuthenticationConfiguration authentication;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(HotRodServerConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet(),
            PROXY_HOST, PROXY_PORT, TOPOLOGY_STATE_TRANSFER, TOPOLOGY_AWAIT_INITIAL_TRANSFER, TOPOLOGY_LOCK_TIMEOUT, TOPOLOGY_REPL_TIMEOUT);
   }

   HotRodServerConfiguration(AttributeSet attributes, SslConfiguration ssl, AuthenticationConfiguration authentication) {
      super(attributes, ssl);
      this.authentication = authentication;

      proxyHost = attributes.attribute(PROXY_HOST);
      proxyPort = attributes.attribute(PROXY_PORT);
      topologyLockTimeout = attributes.attribute(TOPOLOGY_LOCK_TIMEOUT);
      topologyReplTimeout = attributes.attribute(TOPOLOGY_REPL_TIMEOUT);
      topologyAwaitInitialTransfer = attributes.attribute(TOPOLOGY_AWAIT_INITIAL_TRANSFER);
      topologyStateTransfer = attributes.attribute(TOPOLOGY_STATE_TRANSFER);
   }

   public String proxyHost() {
      return proxyHost.get();
   }

   public String publicHost() {
      return proxyHost.isNull() ? host() : proxyHost.get();
   }

   public int proxyPort() {
      return proxyPort.get();
   }

   public int publicPort() {
      return proxyPort.isModified() ? proxyPort.get() : port();
   }

   public String topologyCacheName() {
      String name = name();
      return TOPOLOGY_CACHE_NAME_PREFIX + (name.length() > 0 ? "_" + name : name);
   }

   public long topologyLockTimeout() {
      return topologyLockTimeout.get();
   }

   public long topologyReplTimeout() {
      return topologyReplTimeout.get();
   }

   public boolean topologyAwaitInitialTransfer() {
      return topologyAwaitInitialTransfer.get();
   }

   public boolean topologyStateTransfer() {
      return topologyStateTransfer.get();
   }

   public AuthenticationConfiguration authentication() {
      return authentication;
   }

   @Override
   public String toString() {
      return "HotRodServerConfiguration[" + attributes + "]";
   }

}

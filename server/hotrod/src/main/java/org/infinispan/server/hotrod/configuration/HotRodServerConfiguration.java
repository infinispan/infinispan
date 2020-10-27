package org.infinispan.server.hotrod.configuration;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.core.configuration.EncryptionConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.hotrod.HotRodServer;

@BuiltBy(HotRodServerConfigurationBuilder.class)
@ConfigurationFor(HotRodServer.class)
public class HotRodServerConfiguration extends ProtocolServerConfiguration {
   public static final String TOPOLOGY_CACHE_NAME_PREFIX = "___hotRodTopologyCache";

   public static final AttributeDefinition<String> PROXY_HOST = AttributeDefinition.builder("externalHost", null, String.class).immutable().build();
   public static final AttributeDefinition<Integer> PROXY_PORT = AttributeDefinition.builder("externalPort", -1).immutable().build();
   // The Hot Rod server has a different default
   public static final AttributeDefinition<Integer> WORKER_THREADS = AttributeDefinition.builder("worker-threads", 160).immutable().build();

   private final Attribute<String> proxyHost;
   private final Attribute<Integer> proxyPort;

   private final TopologyCacheConfiguration topologyCache;
   private final AuthenticationConfiguration authentication;
   private final EncryptionConfiguration encryption;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(HotRodServerConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet(),
            WORKER_THREADS, PROXY_HOST, PROXY_PORT);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return Arrays.asList(topologyCache, authentication, encryption);
   }

   public static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("hotrod-connector");

   HotRodServerConfiguration(AttributeSet attributes,
                             TopologyCacheConfiguration topologyCache,
                             SslConfiguration ssl, AuthenticationConfiguration authentication,
                             EncryptionConfiguration encryption) {
      super(attributes, ssl);
      this.topologyCache = topologyCache;
      this.authentication = authentication;
      this.encryption = encryption;
      proxyHost = attributes.attribute(PROXY_HOST);
      proxyPort = attributes.attribute(PROXY_PORT);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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
      return topologyCache.lockTimeout();
   }

   public long topologyReplTimeout() {
      return topologyCache.replicationTimeout();
   }

   public boolean topologyAwaitInitialTransfer() {
      return topologyCache.awaitInitialTransfer();
   }

   /**
    * @deprecated since 11.0. To be removed in 14.0 ISPN-11864 with no direct replacement.
    */
   @Deprecated
   public boolean topologyStateTransfer() {
      return !topologyCache.lazyRetrieval();
   }

   public AuthenticationConfiguration authentication() {
      return authentication;
   }

   public TopologyCacheConfiguration topologyCache() {
      return topologyCache;
   }

   public EncryptionConfiguration encryption() {
      return encryption;
   }

   @Override
   public String toString() {
      return "HotRodServerConfiguration{" +
            "proxyHost=" + proxyHost +
            ", proxyPort=" + proxyPort +
            ", topologyCache=" + topologyCache +
            ", authentication=" + authentication +
            ", encryption=" + encryption +
            '}';
   }

}

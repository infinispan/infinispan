package org.infinispan.server.hotrod.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.server.hotrod.event.ConverterFactory;
import org.infinispan.server.hotrod.event.KeyValueFilterFactory;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

import java.util.Map;

@BuiltBy(HotRodServerConfigurationBuilder.class)
public class HotRodServerConfiguration extends ProtocolServerConfiguration {
   public static final String TOPOLOGY_CACHE_NAME_PREFIX = "___hotRodTopologyCache";
   private final String proxyHost;
   private final int proxyPort;
   private final String topologyCacheName;
   private final long topologyLockTimeout;
   private final long topologyReplTimeout;
   private final boolean topologyAwaitInitialTransfer;
   private final boolean topologyStateTransfer;
   private final AuthenticationConfiguration authentication;
   private final Map<String, KeyValueFilterFactory> keyValueFilterFactories;
   private final Map<String, ConverterFactory> converterFactories;
   private final Marshaller marshaller;

   HotRodServerConfiguration(String defaultCacheName, String proxyHost, int proxyPort, long topologyLockTimeout, long topologyReplTimeout, boolean topologyAwaitInitialTransfer, boolean topologyStateTransfer,
         String name, String host, int port, int idleTimeout, int recvBufSize, int sendBufSize, SslConfiguration ssl, boolean tcpNoDelay, int workerThreads, AuthenticationConfiguration authentication,
         Map<String, KeyValueFilterFactory> keyValueFilterFactories, Map<String, ConverterFactory> converterFactories, Marshaller marshaller) {
      super(defaultCacheName, name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl, tcpNoDelay, workerThreads);
      this.proxyHost = proxyHost;
      this.proxyPort = proxyPort;
      this.topologyCacheName = TOPOLOGY_CACHE_NAME_PREFIX + (name.length() > 0 ? "_" + name : name);
      this.topologyLockTimeout = topologyLockTimeout;
      this.topologyReplTimeout = topologyReplTimeout;
      this.topologyStateTransfer = topologyStateTransfer;
      this.topologyAwaitInitialTransfer = topologyAwaitInitialTransfer;
      this.authentication = authentication;
      this.keyValueFilterFactories = keyValueFilterFactories;
      this.converterFactories = converterFactories;
      this.marshaller = marshaller;
   }

   public String proxyHost() {
      return proxyHost;
   }

   public int proxyPort() {
      return proxyPort;
   }

   public String topologyCacheName() {
      return topologyCacheName;
   }

   public long topologyLockTimeout() {
      return topologyLockTimeout;
   }

   public long topologyReplTimeout() {
      return topologyReplTimeout;
   }

   public boolean topologyAwaitInitialTransfer() {
      return topologyAwaitInitialTransfer;
   }

   public boolean topologyStateTransfer() {
      return topologyStateTransfer;
   }

   public AuthenticationConfiguration authentication() {
      return authentication;
   }

   public KeyValueFilterFactory keyValueFilterFactory(String name) {
      return keyValueFilterFactories.get(name);
   }

   public ConverterFactory converterFactory(String name) {
      return converterFactories.get(name);
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   @Override
   public String toString() {
      return "HotRodServerConfiguration [proxyHost=" + proxyHost + ", proxyPort=" + proxyPort + ", topologyCacheName="
            + topologyCacheName + ", topologyLockTimeout=" + topologyLockTimeout + ", topologyReplTimeout="
            + topologyReplTimeout + ", topologyAwaitInitialTransfer=" + topologyAwaitInitialTransfer
            + ", topologyStateTransfer=" + topologyStateTransfer + ", authentication=" + authentication
            + ", " + super.toString() + "]";
   }
}

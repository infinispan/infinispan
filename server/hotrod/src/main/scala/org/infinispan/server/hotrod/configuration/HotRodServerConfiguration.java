package org.infinispan.server.hotrod.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

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

   HotRodServerConfiguration(String proxyHost, int proxyPort, long topologyLockTimeout, long topologyReplTimeout, boolean topologyAwaitInitialTransfer, boolean topologyStateTransfer,
         String name, String host, int port, int idleTimeout, int recvBufSize, int sendBufSize, SslConfiguration ssl, boolean tcpNoDelay, int workerThreads) {
      super(name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl, tcpNoDelay, workerThreads);
      this.proxyHost = proxyHost;
      this.proxyPort = proxyPort;
      this.topologyCacheName = TOPOLOGY_CACHE_NAME_PREFIX + (name.length() > 0 ? "_" + name : name);
      this.topologyLockTimeout = topologyLockTimeout;
      this.topologyReplTimeout = topologyReplTimeout;
      this.topologyStateTransfer = topologyStateTransfer;
      this.topologyAwaitInitialTransfer = topologyAwaitInitialTransfer;
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

   @Override
   public String toString() {
      return "HotRodServerConfiguration [proxyHost=" + proxyHost + ", proxyPort=" + proxyPort + ", topologyCacheName=" + topologyCacheName + ", topologyLockTimeout="
            + topologyLockTimeout + ", topologyReplTimeout=" + topologyReplTimeout + ", topologyAwaitInitialTransfer=" + topologyAwaitInitialTransfer + ", topologyStateTransfer="
            + topologyStateTransfer + "]";
   }
}

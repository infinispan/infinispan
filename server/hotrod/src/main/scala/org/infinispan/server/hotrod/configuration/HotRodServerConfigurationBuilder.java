package org.infinispan.server.hotrod.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.LockingConfigurationBuilder;
import org.infinispan.configuration.cache.StateTransferConfigurationBuilder;
import org.infinispan.configuration.cache.SyncConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;

/**
 * HotRodServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class HotRodServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<HotRodServerConfiguration, HotRodServerConfigurationBuilder> implements
      Builder<HotRodServerConfiguration> {
   private String proxyHost;
   private int proxyPort = -1;
   private long topologyLockTimeout = 10000L;
   private long topologyReplTimeout = 10000L;
   private boolean topologyAwaitInitialTransfer = true;
   private boolean topologyStateTransfer = true;

   public HotRodServerConfigurationBuilder() {
      super(11222);
   }

   @Override
   public HotRodServerConfigurationBuilder self() {
      return this;
   }

   /**
    * Sets the external address of this node, i.e. the address which clients will connect to
    */
   public HotRodServerConfigurationBuilder proxyHost(String proxyHost) {
      this.proxyHost = proxyHost;
      return this;
   }

   /**
    * Sets the external port of this node, i.e. the port which clients will connect to
    */
   public HotRodServerConfigurationBuilder proxyPort(int proxyPort) {
      this.proxyPort = proxyPort;
      return this;
   }

   /**
    * Configures the lock acquisition timeout for the topology cache. See {@link LockingConfigurationBuilder#lockAcquisitionTimeout(long)}. Defaults to 10 seconds
    */
   public HotRodServerConfigurationBuilder topologyLockTimeout(long topologyLockTimeout) {
      this.topologyLockTimeout = topologyLockTimeout;
      return this;
   }

   /**
    * Configures the replication timeout for the topology cache. See {@link SyncConfigurationBuilder#replTimeout(long)}. Defaults to 10 seconds
    */
   public HotRodServerConfigurationBuilder topologyReplTimeout(long topologyReplTimeout) {
      this.topologyReplTimeout = topologyReplTimeout;
      return this;
   }

   /**
    * Configures whether to enable waiting for initial state transfer for the topology cache. See {@link StateTransferConfigurationBuilder#awaitInitialTransfer(boolean)}
    */
   public HotRodServerConfigurationBuilder topologyAwaitInitialTransfer(boolean topologyAwaitInitialTransfer) {
      this.topologyAwaitInitialTransfer = topologyAwaitInitialTransfer;
      return this;
   }

   /**
    * Configures whether to enable state transfer for the topology cache. If disabled, a {@link org.infinispan.persistence.cluster.ClusterLoader} will be used to lazily retrieve topology information from the other nodes.
    * Defaults to true.
    */
   public HotRodServerConfigurationBuilder topologyStateTransfer(boolean topologyStateTransfer) {
      this.topologyStateTransfer = topologyStateTransfer;
      return this;
   }

   @Override
   public HotRodServerConfiguration create() {
      return new HotRodServerConfiguration(proxyHost, proxyPort, topologyLockTimeout, topologyReplTimeout, topologyAwaitInitialTransfer, topologyStateTransfer, name, host, port, idleTimeout,
            recvBufSize, sendBufSize, ssl.create(), tcpNoDelay, workerThreads);
   }

   @Override
   public HotRodServerConfigurationBuilder read(HotRodServerConfiguration template) {
      super.read(template);
      this.proxyHost = template.proxyHost();
      this.proxyPort = template.proxyPort();
      this.topologyLockTimeout = template.topologyLockTimeout();
      this.topologyReplTimeout = template.topologyReplTimeout();
      this.topologyAwaitInitialTransfer = template.topologyAwaitInitialTransfer();
      this.topologyStateTransfer = template.topologyStateTransfer();
      return this;
   }

   @Override
   public void validate() {
      super.validate();
      if (proxyHost == null) {
         proxyHost = host;
      }
      if (proxyPort < 0) {
         proxyPort = port;
      }
   }

   public HotRodServerConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public HotRodServerConfiguration build() {
      return build(true);
   }

}

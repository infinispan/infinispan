/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.hotrod.configuration;

import java.util.Properties;

import org.infinispan.configuration.Builder;
import org.infinispan.configuration.cache.LockingConfigurationBuilder;
import org.infinispan.configuration.cache.SyncConfigurationBuilder;
import org.infinispan.loaders.cluster.ClusterCacheLoader;
import org.infinispan.server.core.Main;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.hotrod.logging.JavaLog;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.logging.LogFactory;

/**
 * HotRodServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class HotRodServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<HotRodServerConfiguration, HotRodServerConfigurationBuilder> implements
      Builder<HotRodServerConfiguration> {
   private static final JavaLog log = LogFactory.getLog(HotRodServerConfigurationBuilder.class, JavaLog.class);
   private String proxyHost;
   private int proxyPort = -1;
   private long topologyLockTimeout = 10000L;
   private long topologyReplTimeout = 10000L;
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
    * Configures whether to enable state transfer for the topology cache. If disabled, a {@link ClusterCacheLoader} will be used to lazily retrieve topology information from the other nodes.
    * Defaults to true.
    */
   public HotRodServerConfigurationBuilder topologyStateTransfer(boolean topologyStateTransfer) {
      this.topologyStateTransfer = topologyStateTransfer;
      return this;
   }

   @Override
   public HotRodServerConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);
      super.withProperties(typed);

      if (typed != null) {
         if (typed.containsKey(Main.PROP_KEY_PROXY_HOST())) {
            this.proxyHost(typed.getProperty(Main.PROP_KEY_PROXY_HOST(), proxyHost, true));
         }
         this.proxyPort(typed.getIntProperty(Main.PROP_KEY_PROXY_PORT(), proxyPort, true));
         this.topologyLockTimeout(typed.getLongProperty(Main.PROP_KEY_TOPOLOGY_LOCK_TIMEOUT(), topologyLockTimeout, true));
         this.topologyReplTimeout(typed.getLongProperty(Main.PROP_KEY_TOPOLOGY_LOCK_TIMEOUT(), topologyReplTimeout, true));
         this.topologyStateTransfer(typed.getBooleanProperty(Main.PROP_KEY_TOPOLOGY_STATE_TRANSFER(), topologyStateTransfer, true));
         if (typed.containsKey(Main.PROP_KEY_TOPOLOGY_UPDATE_TIMEOUT())) {
            log.topologyUpdateTimeoutIgnored();
         }
      }

      return this;
   }

   @Override
   public HotRodServerConfiguration create() {
      return new HotRodServerConfiguration(proxyHost, proxyPort, topologyLockTimeout, topologyReplTimeout, topologyStateTransfer, name, host, port, idleTimeout,
            recvBufSize, sendBufSize, ssl.create(), tcpNoDelay, workerThreads);
   }

   @Override
   public HotRodServerConfigurationBuilder read(HotRodServerConfiguration template) {
      super.read(template);
      this.proxyHost = template.proxyHost();
      this.proxyPort = template.proxyPort();
      this.topologyLockTimeout = template.topologyLockTimeout();
      this.topologyReplTimeout = template.topologyReplTimeout();
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

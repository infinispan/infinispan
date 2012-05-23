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
import org.infinispan.server.core.Main;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.util.TypedProperties;

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
   private boolean topologyStateTransfer = true;
   private long topologyUpdateTimeout = 30000L;

   public HotRodServerConfigurationBuilder() {
      super(11222);
   }

   @Override
   public HotRodServerConfigurationBuilder self() {
      return this;
   }

   public HotRodServerConfigurationBuilder proxyHost(String proxyHost) {
      this.proxyHost = proxyHost;
      return this;
   }

   public HotRodServerConfigurationBuilder proxyPort(int proxyPort) {
      this.proxyPort = proxyPort;
      return this;
   }

   public HotRodServerConfigurationBuilder topologyLockTimeout(long topologyLockTimeout) {
      this.topologyLockTimeout = topologyLockTimeout;
      return this;
   }

   public HotRodServerConfigurationBuilder topologyReplTimeout(long topologyReplTimeout) {
      this.topologyReplTimeout = topologyReplTimeout;
      return this;
   }

   public HotRodServerConfigurationBuilder topologyStateTransfer(boolean topologyStateTransfer) {
      this.topologyStateTransfer = topologyStateTransfer;
      return this;
   }

   public HotRodServerConfigurationBuilder topologyUpdateTimeout(long topologyUpdateTimeout) {
      this.topologyUpdateTimeout = topologyUpdateTimeout;
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
         this.topologyUpdateTimeout(typed.getLongProperty(Main.PROP_KEY_TOPOLOGY_UPDATE_TIMEOUT(), topologyUpdateTimeout, true));
      }

      return this;
   }

   @Override
   public HotRodServerConfiguration create() {
      return new HotRodServerConfiguration(proxyHost, proxyPort, topologyLockTimeout, topologyReplTimeout, topologyStateTransfer, topologyUpdateTimeout, host, port, idleTimeout,
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
      this.topologyUpdateTimeout = template.topologyUpdateTimeout();
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

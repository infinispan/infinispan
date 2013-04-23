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

import org.infinispan.configuration.BuiltBy;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

@BuiltBy(HotRodServerConfigurationBuilder.class)
public class HotRodServerConfiguration extends ProtocolServerConfiguration {
   private final String proxyHost;
   private final int proxyPort;
   private final long topologyLockTimeout;
   private final long topologyReplTimeout;
   private final boolean topologyStateTransfer;
   private final long topologyUpdateTimeout;

   HotRodServerConfiguration(String proxyHost, int proxyPort, long topologyLockTimeout, long topologyReplTimeout, boolean topologyStateTransfer, long topologyUpdateTimeout,
         String host, int port, int idleTimeout, int recvBufSize, int sendBufSize, SslConfiguration ssl, boolean tcpNoDelay, int workerThreads) {
      super(host, port, idleTimeout, recvBufSize, sendBufSize, ssl, tcpNoDelay, workerThreads);
      this.proxyHost = proxyHost;
      this.proxyPort = proxyPort;
      this.topologyLockTimeout = topologyLockTimeout;
      this.topologyReplTimeout = topologyReplTimeout;
      this.topologyStateTransfer = topologyStateTransfer;
      this.topologyUpdateTimeout = topologyUpdateTimeout;
   }

   public String proxyHost() {
      return proxyHost;
   }

   public int proxyPort() {
      return proxyPort;
   }

   public long topologyLockTimeout() {
      return topologyLockTimeout;
   }

   public long topologyReplTimeout() {
      return topologyReplTimeout;
   }

   public boolean topologyStateTransfer() {
      return topologyStateTransfer;
   }

   public long topologyUpdateTimeout() {
      return topologyUpdateTimeout;
   }
   @Override
   public String toString() {
      return "HotRodServerConfiguration [proxyHost=" + proxyHost + ", proxyPort=" + proxyPort + ", topologyLockTimeout=" + topologyLockTimeout + ", topologyReplTimeout="
            + topologyReplTimeout + ", topologyStateTransfer=" + topologyStateTransfer + ", topologyUpdateTimeout=" + topologyUpdateTimeout + ", " + super.toString()
            + "]";
   }
}

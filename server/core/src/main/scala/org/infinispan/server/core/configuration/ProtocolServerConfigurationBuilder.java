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
package org.infinispan.server.core.configuration;

import java.util.Properties;

import org.infinispan.configuration.Builder;
import org.infinispan.server.core.Main;
import org.infinispan.server.core.logging.JavaLog;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.logging.LogFactory;

public abstract class ProtocolServerConfigurationBuilder<T extends ProtocolServerConfiguration, S extends ProtocolServerConfigurationChildBuilder<T, S>> implements
      ProtocolServerConfigurationChildBuilder<T, S>, Builder<T> {
   private static final JavaLog log = LogFactory.getLog(ProtocolServerConfigurationBuilder.class, JavaLog.class);
   protected String name = "";
   protected String host = "127.0.0.1";
   protected int port = -1;
   protected int idleTimeout = -1;
   protected int recvBufSize = 0;
   protected int sendBufSize = 0;
   protected final SslConfigurationBuilder ssl;
   protected boolean tcpNoDelay = true;
   protected int workerThreads = 2 * Runtime.getRuntime().availableProcessors();

   protected ProtocolServerConfigurationBuilder(int port) {
      this.port = port;
      this.ssl = new SslConfigurationBuilder();
   }

   @Override
   public S name(String name) {
      this.name = name;
      return this.self();
   }

   @Override
   public S host(String host) {
      this.host = host;
      return this.self();
   }

   @Override
   public S port(int port) {
      this.port = port;
      return this.self();
   }

   @Override
   public S idleTimeout(int idleTimeout) {
      this.idleTimeout = idleTimeout;
      return this.self();
   }

   @Override
   public S tcpNoDelay(boolean tcpNoDelay) {
      this.tcpNoDelay = tcpNoDelay;
      return this.self();
   }

   @Override
   public S recvBufSize(int recvBufSize) {
      this.recvBufSize = recvBufSize;
      return this.self();
   }

   @Override
   public S sendBufSize(int sendBufSize) {
      this.sendBufSize = sendBufSize;
      return this.self();
   }

   @Override
   public SslConfigurationBuilder ssl() {
      return ssl;
   }

   @Override
   public S withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);

      if (typed != null) {
         this.host(typed.getProperty(Main.PROP_KEY_HOST(), host, true));
         this.port(typed.getIntProperty(Main.PROP_KEY_PORT(), port, true));
         this.idleTimeout(typed.getIntProperty(Main.PROP_KEY_IDLE_TIMEOUT(), idleTimeout, true));
         this.recvBufSize(typed.getIntProperty(Main.PROP_KEY_RECV_BUF_SIZE(), recvBufSize, true));
         this.sendBufSize(typed.getIntProperty(Main.PROP_KEY_SEND_BUF_SIZE(), sendBufSize, true));
         this.tcpNoDelay(typed.getBooleanProperty(Main.PROP_KEY_TCP_NO_DELAY(), tcpNoDelay, true));
         this.workerThreads(typed.getIntProperty(Main.PROP_KEY_WORKER_THREADS(), workerThreads, true));
      }

      return this.self();
   }

   @Override
   public S workerThreads(int workerThreads) {
      this.workerThreads = workerThreads;
      return this.self();
   }

   @Override
   public void validate() {
      ssl.validate();
      if (idleTimeout < -1) {
         throw log.illegalIdleTimeout(idleTimeout);
      }
      if (sendBufSize < 0) {
         throw log.illegalSendBufferSize(sendBufSize);
      }
      if (recvBufSize < 0) {
         throw log.illegalReceiveBufferSize(recvBufSize);
      }
      if (workerThreads < 0) {
         throw log.illegalWorkerThreads(workerThreads);
      }
   }

   @Override
   public Builder<?> read(T template) {
      this.name = template.name();
      this.host = template.host();
      this.port = template.port();
      this.idleTimeout = template.idleTimeout();
      this.recvBufSize = template.recvBufSize();
      this.sendBufSize = template.sendBufSize();
      this.tcpNoDelay = template.tcpNoDelay();
      this.workerThreads = template.workerThreads();
      this.ssl.read(template.ssl());
      return this;
   }
}

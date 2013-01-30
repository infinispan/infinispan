/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.infinispan.client.hotrod.impl.operations.PingOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TransportObjectFactory
      extends BaseKeyedPoolableObjectFactory<SocketAddress, TcpTransport> {

   private static final Log log = LogFactory.getLog(TransportObjectFactory.class);
   private final TcpTransportFactory tcpTransportFactory;
   private final AtomicInteger topologyId;
   private final boolean pingOnStartup;
   private volatile boolean firstPingExecuted = false;
   private final Codec codec;

   public TransportObjectFactory(Codec codec, TcpTransportFactory tcpTransportFactory, AtomicInteger topologyId, boolean pingOnStartup) {
      this.tcpTransportFactory = tcpTransportFactory;
      this.topologyId = topologyId;
      this.pingOnStartup = pingOnStartup;
      this.codec = codec;
   }

   @Override
   public TcpTransport makeObject(SocketAddress address) throws Exception {
      TcpTransport tcpTransport = new TcpTransport(address, tcpTransportFactory);
      if (log.isTraceEnabled()) {
         log.tracef("Created tcp transport: %s", tcpTransport);
      }
      if (pingOnStartup && !firstPingExecuted) {
         log.trace("Executing first ping!");
         firstPingExecuted = true;

         // Don't ignore exceptions from ping() command, since
         // they indicate that the transport instance is invalid.
         ping(tcpTransport, topologyId);
      }
      return tcpTransport;
   }

   private PingOperation.PingResult ping(TcpTransport tcpTransport, AtomicInteger topologyId) {
      PingOperation po = new PingOperation(codec, topologyId, tcpTransport);
      return po.execute();
   }

   /**
    * This will be called by the test thread when testWhileIdle==true.
    */
   @Override
   public boolean validateObject(SocketAddress address, TcpTransport transport) {
      if (log.isTraceEnabled()) {
         log.tracef("About to validate(ping) connection to server %s. TcpTransport is %s",
               address, transport);
      }
      return ping(transport, topologyId) == PingOperation.PingResult.SUCCESS;
   }

   @Override
   public void destroyObject(SocketAddress address, TcpTransport transport) throws Exception {
      if (log.isTraceEnabled()) {
         log.tracef("About to destroy tcp transport: %s", transport);
      }
      transport.destroy();
   }

   @Override
   public void activateObject(SocketAddress address, TcpTransport transport) throws Exception {
      super.activateObject(address, transport);
      if (log.isTraceEnabled()) {
         log.tracef("Fetching from pool: %s", transport);
      }
   }

   @Override
   public void passivateObject(SocketAddress address, TcpTransport transport) throws Exception {
      super.passivateObject(address, transport);
      if (log.isTraceEnabled()) {
         log.tracef("Returning to pool: %s", transport);
      }
   }
}

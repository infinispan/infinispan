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

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.infinispan.client.hotrod.impl.operations.PingOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TransportObjectFactory extends BaseKeyedPoolableObjectFactory {

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
   public Object makeObject(Object key) throws Exception {
      InetSocketAddress serverAddress = (InetSocketAddress) key;
      TcpTransport tcpTransport = new TcpTransport(serverAddress, tcpTransportFactory);
      if (log.isTraceEnabled()) {
         log.tracef("Created tcp transport: %s", tcpTransport);
      }
      if (pingOnStartup && !firstPingExecuted) {
         log.trace("Executing first ping!");
         firstPingExecuted = true;
         try {
            ping(tcpTransport, topologyId);
         } catch (Exception e) {
            log.tracef("Ignoring ping request failure during ping on startup: %s", e.getMessage());
         }
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
   public boolean validateObject(Object key, Object obj) {
      TcpTransport transport = (TcpTransport) obj;
      if (log.isTraceEnabled()) {
         log.tracef("About to validate(ping) connection to server %s. TcpTransport is %s", key, transport);
      }
      return ping(transport, topologyId) == PingOperation.PingResult.SUCCESS;
   }

   @Override
   public void destroyObject(Object key, Object obj) throws Exception {
      if (log.isTraceEnabled()) {
         log.tracef("About to destroy tcp transport: %s", obj);
      }
      TcpTransport transport = (TcpTransport) obj;
      transport.destroy();
   }

   @Override
   public void activateObject(Object key, Object obj) throws Exception {
      super.activateObject(key, obj);
      if (log.isTraceEnabled()) {
         log.tracef("Fetching from pool: %s", obj);
      }
   }

   @Override
   public void passivateObject(Object key, Object obj) throws Exception {
      super.passivateObject(key, obj);
      if (log.isTraceEnabled()) {
         log.tracef("Returning to pool: %s", obj);
      }
   }
}

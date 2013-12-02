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
package org.infinispan.server.endpoint.subsystem;

import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.dmr.ModelNode;

public abstract class ProtocolServiceSubsystemAdd extends AbstractAddStepHandler {
   private static final int DEFAULT_WORKER_THREADS = 160;

   protected String getServiceName(ModelNode config) {
      return config.hasDefined(ModelKeys.NAME) ? config.get(ModelKeys.NAME).asString() : null;
   }

   protected String getSocketBindingName(ModelNode config) {
      return config.hasDefined(ModelKeys.SOCKET_BINDING) ? config.get(ModelKeys.SOCKET_BINDING).asString() : null;
   }

   protected String getCacheContainerName(ModelNode config) {
      return config.hasDefined(ModelKeys.CACHE_CONTAINER) ? config.get(ModelKeys.CACHE_CONTAINER).asString() : null;
   }

   protected void configureProtocolServer(ProtocolServerConfigurationBuilder<?, ?> builder, ModelNode config) {
      if (config.hasDefined(ModelKeys.NAME)) {
         builder.name(config.get(ModelKeys.NAME).asString());
      }

      builder.workerThreads(config.hasDefined(ModelKeys.WORKER_THREADS) ? config.get(ModelKeys.WORKER_THREADS).asInt() : DEFAULT_WORKER_THREADS);

      if (config.hasDefined(ModelKeys.IDLE_TIMEOUT)) {
         builder.idleTimeout(config.get(ModelKeys.IDLE_TIMEOUT).asInt());
      }
      if (config.hasDefined(ModelKeys.TCP_NODELAY)) {
         builder.tcpNoDelay(config.get(ModelKeys.TCP_NODELAY).asBoolean());
      }
      if (config.hasDefined(ModelKeys.SEND_BUFFER_SIZE)) {
         builder.sendBufSize(config.get(ModelKeys.SEND_BUFFER_SIZE).asInt());
      }
      if (config.hasDefined(ModelKeys.RECEIVE_BUFFER_SIZE)) {
         builder.recvBufSize(config.get(ModelKeys.RECEIVE_BUFFER_SIZE).asInt());
      }
   }

}

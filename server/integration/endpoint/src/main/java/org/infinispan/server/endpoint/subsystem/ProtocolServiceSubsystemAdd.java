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

import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.dmr.ModelNode;

public abstract class ProtocolServiceSubsystemAdd extends AbstractAddStepHandler {
   protected String getServiceName(ModelNode config) {
      return config.hasDefined(ModelKeys.NAME) ? config.get(ModelKeys.NAME).asString() : null;
   }

   protected String getSocketBindingName(ModelNode config) {
      return config.hasDefined(ModelKeys.SOCKET_BINDING) ? config.get(ModelKeys.SOCKET_BINDING).asString() : null;
   }

   protected String getCacheContainerName(ModelNode config) {
      return config.hasDefined(ModelKeys.CACHE_CONTAINER) ? config.get(ModelKeys.CACHE_CONTAINER).asString() : null;
   }

   protected void configureProtocolServer(OperationContext context, ProtocolServerConfigurationBuilder<?, ?> builder, ModelNode config) throws OperationFailedException {
      if (config.hasDefined(ModelKeys.NAME)) {
         builder.name(config.get(ModelKeys.NAME).asString());
      }
      builder.workerThreads(ProtocolServerConnectorResource.WORKER_THREADS.resolveModelAttribute(context, config).asInt());
      builder.idleTimeout(ProtocolServerConnectorResource.IDLE_TIMEOUT.resolveModelAttribute(context, config).asInt());
      builder.tcpNoDelay(ProtocolServerConnectorResource.TCP_NODELAY.resolveModelAttribute(context, config).asBoolean());
      builder.tcpKeepAlive(ProtocolServerConnectorResource.TCP_KEEPALIVE.resolveModelAttribute(context, config).asBoolean());
      builder.recvBufSize(ProtocolServerConnectorResource.RECEIVE_BUFFER_SIZE.resolveModelAttribute(context, config).asInt());
      builder.sendBufSize(ProtocolServerConnectorResource.SEND_BUFFER_SIZE.resolveModelAttribute(context, config).asInt());
      if (config.hasDefined(ModelKeys.IGNORED_CACHES)) {
         Set<String> ignoredCaches = config.get(ModelKeys.IGNORED_CACHES).asList()
                 .stream().map(ModelNode::asString).collect(Collectors.toSet());
         builder.ignoredCaches(ignoredCaches);
      }
   }

}

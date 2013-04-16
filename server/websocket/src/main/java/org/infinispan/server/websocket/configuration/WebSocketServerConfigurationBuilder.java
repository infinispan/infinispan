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
package org.infinispan.server.websocket.configuration;

import org.infinispan.configuration.Builder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;

/**
 * WebSocketServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class WebSocketServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<WebSocketServerConfiguration, WebSocketServerConfigurationBuilder> implements
      Builder<WebSocketServerConfiguration> {

   public WebSocketServerConfigurationBuilder() {
      super(8181);
   }

   @Override
   public WebSocketServerConfigurationBuilder self() {
      return this;
   }

   @Override
   public WebSocketServerConfiguration create() {
      return new WebSocketServerConfiguration(host, port, idleTimeout, recvBufSize, sendBufSize, tcpNoDelay, workerThreads);
   }

   public WebSocketServerConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public WebSocketServerConfiguration build() {
      return build(true);
   }

}

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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

import org.infinispan.configuration.Self;

public interface ProtocolServerConfigurationChildBuilder<T extends ProtocolServerConfiguration, S extends ProtocolServerConfigurationChildBuilder<T,S>> extends Self<S> {
   /**
    * Specifies the host or IP address on which this server will listen
    */
   S host(String host);

   /**
    * Specifies the port on which this server will listen
    */
   S port(int port);

   /**
    * Specifies the maximum time that connections from client will be kept open without activity
    */
   S idleTimeout(int idleTimeout);

   /**
    * Affects TCP NODELAY on the TCP stack. Defaults to enabled
    */
   S tcpNoDelay(boolean tcpNoDelay);

   /**
    * Sets the size of the receive buffer
    */
   S recvBufSize(int recvBufSize);

   /**
    * Sets the size of the send buffer
    */
   S sendBufSize(int sendBufSize);

   /**
    * Configures SSL
    */
   SslConfigurationBuilder ssl();

   /**
    * Configures this builder using the specified properties
    */
   S withProperties(Properties properties);

   /**
    * Sets the number of worker threads
    */
   S workerThreads(int workerThreads);

   /**
    * Builds a configuration object
    */
   T build();

}

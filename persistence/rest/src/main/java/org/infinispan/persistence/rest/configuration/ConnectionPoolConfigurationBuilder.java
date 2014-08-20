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
package org.infinispan.persistence.rest.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 *
 * ConnectionPoolConfigurationBuilder. Specifies connection pooling properties for the HttpClient
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public class ConnectionPoolConfigurationBuilder extends AbstractRestStoreConfigurationChildBuilder<RestStoreConfigurationBuilder> implements
      Builder<ConnectionPoolConfiguration> {
   private int connectionTimeout = 60000;
   private int maxConnectionsPerHost = 4;
   private int maxTotalConnections = 20;
   private int bufferSize = 8192;
   private int socketTimeout = 60000;
   private boolean tcpNoDelay = true;

   ConnectionPoolConfigurationBuilder(RestStoreConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Controls the maximum number of connections per host.
    */
   public ConnectionPoolConfigurationBuilder maxConnectionsPerHost(int maxConnectionsPerHost) {
      this.maxConnectionsPerHost = maxConnectionsPerHost;
      return this;
   }

   public ConnectionPoolConfigurationBuilder maxTotalConnections(int maxTotalConnections) {
      this.maxTotalConnections = maxTotalConnections;
      return this;
   }

   public ConnectionPoolConfigurationBuilder connectionTimeout(int connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      return this;
   }

   public ConnectionPoolConfigurationBuilder bufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
   }

   public ConnectionPoolConfigurationBuilder socketTimeout(int socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
   }

   public ConnectionPoolConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      this.tcpNoDelay = tcpNoDelay;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public ConnectionPoolConfiguration create() {
      return new ConnectionPoolConfiguration(connectionTimeout, maxConnectionsPerHost, maxTotalConnections, bufferSize, socketTimeout, tcpNoDelay);
   }

   @Override
   public ConnectionPoolConfigurationBuilder read(ConnectionPoolConfiguration template) {
      this.connectionTimeout = template.connectionTimeout();
      this.maxConnectionsPerHost = template.maxConnectionsPerHost();
      this.maxTotalConnections = template.maxTotalConnections();
      this.bufferSize = template.bufferSize();
      this.socketTimeout = template.socketTimeout();
      this.tcpNoDelay = template.tcpNoDelay();
      return this;
   }
}

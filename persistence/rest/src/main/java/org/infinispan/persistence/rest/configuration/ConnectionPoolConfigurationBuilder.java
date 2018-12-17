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

import static org.infinispan.persistence.rest.configuration.ConnectionPoolConfiguration.BUFFER_SIZE;
import static org.infinispan.persistence.rest.configuration.ConnectionPoolConfiguration.CONNECTION_TIMEOUT;
import static org.infinispan.persistence.rest.configuration.ConnectionPoolConfiguration.MAX_CONNECTIONS_PER_HOST;
import static org.infinispan.persistence.rest.configuration.ConnectionPoolConfiguration.MAX_TOTAL_CONNECTIONS;
import static org.infinispan.persistence.rest.configuration.ConnectionPoolConfiguration.SOCKET_TIMEOUT;
import static org.infinispan.persistence.rest.configuration.ConnectionPoolConfiguration.TCP_NO_DELAY;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 *
 * ConnectionPoolConfigurationBuilder. Specifies connection pooling properties for the HttpClient
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public class ConnectionPoolConfigurationBuilder extends AbstractRestStoreConfigurationChildBuilder<RestStoreConfigurationBuilder> implements
      Builder<ConnectionPoolConfiguration>, ConfigurationBuilderInfo {

   ConnectionPoolConfigurationBuilder(RestStoreConfigurationBuilder builder) {
      super(builder, ConnectionPoolConfiguration.attributeDefinitionSet());
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ConnectionPoolConfiguration.ELEMENT_DEFINITION;
   }

   /**
    * Controls the maximum number of connections per host.
    */
   public ConnectionPoolConfigurationBuilder maxConnectionsPerHost(int maxConnectionsPerHost) {
      attributes.attribute(MAX_CONNECTIONS_PER_HOST).set(maxConnectionsPerHost);
      return this;
   }

   public ConnectionPoolConfigurationBuilder maxTotalConnections(int maxTotalConnections) {
      attributes.attribute(MAX_TOTAL_CONNECTIONS).set(maxTotalConnections);
      return this;
   }

   public ConnectionPoolConfigurationBuilder connectionTimeout(int connectionTimeout) {
      attributes.attribute(CONNECTION_TIMEOUT).set(connectionTimeout);
      return this;
   }

   public ConnectionPoolConfigurationBuilder bufferSize(int bufferSize) {
      attributes.attribute(BUFFER_SIZE).set(bufferSize);
      return this;
   }

   public ConnectionPoolConfigurationBuilder socketTimeout(int socketTimeout) {
      attributes.attribute(SOCKET_TIMEOUT).set(socketTimeout);
      return this;
   }

   public ConnectionPoolConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      attributes.attribute(TCP_NO_DELAY).set(tcpNoDelay);
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
      return new ConnectionPoolConfiguration(attributes);
   }

   @Override
   public ConnectionPoolConfigurationBuilder read(ConnectionPoolConfiguration template) {
      maxConnectionsPerHost(template.maxConnectionsPerHost());
      maxTotalConnections(template.maxTotalConnections());
      connectionTimeout(template.connectionTimeout());
      bufferSize(template.bufferSize());
      socketTimeout(template.socketTimeout());
      tcpNoDelay(template.tcpNoDelay());
      return this;
   }
}

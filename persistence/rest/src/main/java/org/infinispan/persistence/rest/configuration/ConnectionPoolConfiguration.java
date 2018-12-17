package org.infinispan.persistence.rest.configuration;

import static org.infinispan.persistence.rest.configuration.Element.CONNECTION_POOL;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * ConnectionPoolConfiguration.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@BuiltBy(ConnectionPoolConfigurationBuilder.class)
public class ConnectionPoolConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<Integer> CONNECTION_TIMEOUT = AttributeDefinition.builder("connectionTimeout", 60000).immutable().build();
   public static final AttributeDefinition<Integer> MAX_CONNECTIONS_PER_HOST = AttributeDefinition.builder("maxConnectionsPerHostTimeout", 4).immutable().xmlName(Attribute.MAX_CONNECTIONS_PER_HOST.getLocalName()).build();
   public static final AttributeDefinition<Integer> MAX_TOTAL_CONNECTIONS = AttributeDefinition.builder("maxTotalConnections", 20).immutable().build();
   public static final AttributeDefinition<Integer> BUFFER_SIZE = AttributeDefinition.builder("bufferSize", 8192).immutable().build();
   public static final AttributeDefinition<Integer> SOCKET_TIMEOUT = AttributeDefinition.builder("socketTimeout", 60000).immutable().build();
   public static final AttributeDefinition<Boolean> TCP_NO_DELAY = AttributeDefinition.builder("tcpNoDelay", true).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ConnectionPoolConfiguration.class, CONNECTION_TIMEOUT, MAX_CONNECTIONS_PER_HOST,
            MAX_TOTAL_CONNECTIONS, BUFFER_SIZE, SOCKET_TIMEOUT, TCP_NO_DELAY);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(CONNECTION_POOL.getLocalName());

   private final AttributeSet attributes;

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public ConnectionPoolConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   public int connectionTimeout() {
      return attributes.attribute(CONNECTION_TIMEOUT).get();
   }

   public int maxConnectionsPerHost() {
      return attributes.attribute(MAX_CONNECTIONS_PER_HOST).get();
   }

   public int maxTotalConnections() {
      return attributes.attribute(MAX_TOTAL_CONNECTIONS).get();
   }

   public int bufferSize() {
      return attributes.attribute(BUFFER_SIZE).get();
   }

   public int socketTimeout() {
      return attributes.attribute(SOCKET_TIMEOUT).get();
   }

   public boolean tcpNoDelay() {
      return attributes.attribute(TCP_NO_DELAY).get();
   }

   @Override
   public String toString() {
      return "ConnectionPoolConfiguration [connectionTimeout=" + connectionTimeout() + ", maxConnectionsPerHost=" + maxConnectionsPerHost() + ", maxTotalConnections="
            + maxTotalConnections() + ", bufferSize=" + bufferSize() + ", socketTimeout=" + socketTimeout() + ", tcpNoDelay="
            + tcpNoDelay() + "]";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ConnectionPoolConfiguration that = (ConnectionPoolConfiguration) o;

      if (connectionTimeout() != that.connectionTimeout()) return false;
      if (maxConnectionsPerHost() != that.maxConnectionsPerHost()) return false;
      if (maxTotalConnections() != that.maxTotalConnections()) return false;
      if (bufferSize() != that.bufferSize()) return false;
      if (socketTimeout() != that.socketTimeout()) return false;
      return tcpNoDelay() == that.tcpNoDelay();

   }

   @Override
   public int hashCode() {
      int result = connectionTimeout();
      result = 31 * result + maxConnectionsPerHost();
      result = 31 * result + maxTotalConnections();
      result = 31 * result + bufferSize();
      result = 31 * result + socketTimeout();
      result = 31 * result + (tcpNoDelay() ? 1 : 0);
      return result;
   }
}

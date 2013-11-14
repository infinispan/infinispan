package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.Self;

/**
 * ProtocolServerConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public interface ProtocolServerConfigurationChildBuilder<T extends ProtocolServerConfiguration, S extends ProtocolServerConfigurationChildBuilder<T,S>> extends Self<S> {
   /**
    * Specifies the cache to use as a default cache for the protocol
    */
   S defaultCacheName(String defaultCacheName);
   /**
    * Specifies a custom name for this server in order to easily distinguish it from other servers, e.g. via JMX. Defaults to the empty string.
    */
   S name(String name);

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
    * Sets the number of worker threads
    */
   S workerThreads(int workerThreads);

   /**
    * Builds a configuration object
    */
   T build();
}

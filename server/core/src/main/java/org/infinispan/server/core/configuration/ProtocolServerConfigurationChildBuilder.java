package org.infinispan.server.core.configuration;

import java.util.Set;

import org.infinispan.commons.configuration.Self;
import org.infinispan.server.core.admin.AdminOperationsHandler;

/**
 * ProtocolServerConfigurationChildBuilder.
 *
 * @param <T> the root configuration object returned by the builder. Must extend {@link ProtocolServerConfiguration}
 * @param <S> the sub-builder this is an implementation of. Must extend ProtocolServerConfigurationChildBuilder
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public interface ProtocolServerConfigurationChildBuilder<T extends ProtocolServerConfiguration, S extends ProtocolServerConfigurationChildBuilder<T,S>>
      extends Self<S> {
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
    * Affects TCP KEEPALIVE on the TCP stack. Defaults to disabled
    */
   S tcpKeepAlive(boolean tcpKeepAlive);

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
   SslConfigurationBuilder<T, S> ssl();

   /**
    * Sets the number of worker threads
    */
   S workerThreads(int workerThreads);

   /**
    * Sets the caches to be ignored
    */
   S ignoredCaches(Set<String> ignoredCaches);

   /**
    * Indicates whether transport implementation should or should not be started.
    */
   S startTransport(boolean startTransport);

   /**
    * Indicates the {@link AdminOperationsHandler} which will be used to handle admin operations
    */
   S adminOperationsHandler(AdminOperationsHandler handler);

   /**
    * Builds a configuration object
    */
   T build();
}

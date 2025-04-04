package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.Self;
import org.infinispan.commons.util.ByteQuantity;
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
public interface ProtocolServerConfigurationChildBuilder<T extends ProtocolServerConfiguration<T, A>, S extends ProtocolServerConfigurationChildBuilder<T, S, A>, A extends AuthenticationConfiguration>
      extends Self<S> {
   /**
    * Specifies the cache to use as a default cache for the protocol
    */
   S defaultCacheName(String defaultCacheName);

   /**
    * Specifies a custom name for this protocol server in order to easily distinguish it from others of the same type on the same server, e.g. via JMX. Defaults to the empty string.
    * The name should be the same across the cluster.
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

   AuthenticationConfigurationBuilder<A> authentication();

   /**
    * Configures SSL
    */
   SslConfigurationBuilder<T, S, A> ssl();

   /**
    * Configures the IP filter rules
    */
   IpFilterConfigurationBuilder<T, S, A> ipFilter();

   /**
    * Sets the number of I/O threads
    */
   S ioThreads(int ioThreads);

   /**
    * Indicates whether transport implementation should or should not be started.
    */
   S startTransport(boolean startTransport);

   /**
    * Indicates the {@link AdminOperationsHandler} which will be used to handle admin operations
    */
   S adminOperationsHandler(AdminOperationsHandler handler);

   /**
    * Indicates the name of socket binding which will be used
    */
   S socketBinding(String name);

   /**
    * Indicates whether this connector was added implicitly
    */
   S implicitConnector(boolean implicitConnector);

   /**
    * The maximum size a request can be, if exceeded the request will be rejected and possibly forcibly close the socket
    * @param maxContentLength the maximum size a request can be, valid values are handled by {@link ByteQuantity}
    * @return this builder
    */
   S maxContentLength(String maxContentLength);

   /**
    * Builds a configuration object
    */
   T build();
}

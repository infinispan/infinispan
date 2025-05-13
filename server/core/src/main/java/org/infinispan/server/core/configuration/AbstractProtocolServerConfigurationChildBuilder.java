package org.infinispan.server.core.configuration;

import org.infinispan.server.core.admin.AdminOperationsHandler;

/**
 * Helper
 *
 * @author Tristan Tarrant
 * @since 9.1
 */

public abstract class AbstractProtocolServerConfigurationChildBuilder<T extends ProtocolServerConfiguration<T, A>, S extends ProtocolServerConfigurationChildBuilder<T, S, A>, A extends AuthenticationConfiguration>
      implements ProtocolServerConfigurationChildBuilder<T, S, A> {
   protected final ProtocolServerConfigurationChildBuilder<T, S, A> builder;

   protected AbstractProtocolServerConfigurationChildBuilder(ProtocolServerConfigurationChildBuilder<T, S, A> builder) {
      this.builder = builder;
   }

   @Override
   public S defaultCacheName(String defaultCacheName) {
      builder.defaultCacheName(defaultCacheName);
      return self();
   }

   @Override
   public S name(String name) {
      builder.name(name);
      return self();
   }

   @Override
   public S host(String host) {
      builder.host(host);
      return self();
   }

   @Override
   public S port(int port) {
      builder.port(port);
      return self();
   }

   @Override
   public S idleTimeout(int idleTimeout) {
      builder.idleTimeout(idleTimeout);
      return self();
   }

   @Override
   public S tcpNoDelay(boolean tcpNoDelay) {
      builder.tcpNoDelay(tcpNoDelay);
      return self();
   }

   @Override
   public S tcpKeepAlive(boolean tcpKeepAlive) {
      builder.tcpKeepAlive(tcpKeepAlive);
      return self();
   }

   @Override
   public S recvBufSize(int recvBufSize) {
      builder.recvBufSize(recvBufSize);
      return self();
   }

   @Override
   public S sendBufSize(int sendBufSize) {
      builder.sendBufSize(sendBufSize);
      return self();
   }

   @Override
   public AuthenticationConfigurationBuilder<A> authentication() {
      return builder.authentication();
   }

   @Override
   public SslConfigurationBuilder<T, S, A> ssl() {
      return builder.ssl();
   }

   @Override
   public S ioThreads(int ioThreads) {
      builder.ioThreads(ioThreads);
      return self();
   }

   @Override
   public S startTransport(boolean startTransport) {
      builder.startTransport(startTransport);
      return self();
   }

   @Override
   public S adminOperationsHandler(AdminOperationsHandler handler) {
      builder.adminOperationsHandler(handler);
      return self();
   }

   @Override
   public S socketBinding(String name) {
      builder.socketBinding(name);
      return self();
   }

   @Override
   public IpFilterConfigurationBuilder<T, S, A> ipFilter() {
      return builder.ipFilter();
   }

   @Override
   public S implicitConnector(boolean implicitConnector) {
      builder.implicitConnector(implicitConnector);
      return self();
   }

   @Override
   public S maxContentLength(String maxContentLength) {
      builder.maxContentLength(maxContentLength);
      return self();
   }

   @Override
   public T build() {
      return builder.build();
   }
}

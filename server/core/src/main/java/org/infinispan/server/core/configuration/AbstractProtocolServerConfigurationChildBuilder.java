package org.infinispan.server.core.configuration;

import java.util.Set;

import org.infinispan.server.core.admin.AdminOperationsHandler;

/**
 * Helper
 *
 * @author Tristan Tarrant
 * @since 9.1
 */

public abstract class AbstractProtocolServerConfigurationChildBuilder<T extends ProtocolServerConfiguration, S extends ProtocolServerConfigurationChildBuilder<T, S>>
      implements ProtocolServerConfigurationChildBuilder<T, S> {
   final protected ProtocolServerConfigurationChildBuilder<T, S> builder;

   protected AbstractProtocolServerConfigurationChildBuilder(ProtocolServerConfigurationChildBuilder<T, S> builder) {
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
   public SslConfigurationBuilder<T, S> ssl() {
      return builder.ssl();
   }

   @Override
   public S workerThreads(int workerThreads) {
      builder.workerThreads(workerThreads);
      return self();
   }

   @Override
   public S ignoredCaches(Set<String> ignoredCaches) {
      builder.ignoredCaches(ignoredCaches);
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
   public T build() {
      return builder.build();
   }
}

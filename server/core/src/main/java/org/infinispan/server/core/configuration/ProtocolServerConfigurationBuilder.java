package org.infinispan.server.core.configuration;

import java.util.Set;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.ADMIN_OPERATION_HANDLER;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.DEFAULT_CACHE_NAME;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.IO_THREADS;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.HOST;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.IDLE_TIMEOUT;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.IGNORED_CACHES;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.NAME;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.PORT;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.RECV_BUF_SIZE;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.SEND_BUF_SIZE;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.START_TRANSPORT;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.TCP_KEEPALIVE;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.TCP_NODELAY;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.WORKER_THREADS;

public abstract class ProtocolServerConfigurationBuilder<T extends ProtocolServerConfiguration, S extends ProtocolServerConfigurationChildBuilder<T, S>>
      implements ProtocolServerConfigurationChildBuilder<T, S>, Builder<T> {
   private static final Log log = LogFactory.getLog(ProtocolServerConfigurationBuilder.class, Log.class);
   protected final AttributeSet attributes;
   protected final SslConfigurationBuilder<T, S> ssl;

   protected ProtocolServerConfigurationBuilder(int port, AttributeSet attributes) {
      this.attributes = attributes;
      this.ssl = new SslConfigurationBuilder(this);
      port(port);

   }

   protected ProtocolServerConfigurationBuilder(int port) {
      this(port, ProtocolServerConfiguration.attributeDefinitionSet());
   }

   @Override
   public S ignoredCaches(Set<String> ignoredCaches) {
      attributes.attribute(IGNORED_CACHES).set(ignoredCaches);
      return this.self();
   }

   @Override
   public S defaultCacheName(String defaultCacheName) {
      attributes.attribute(DEFAULT_CACHE_NAME).set(defaultCacheName);
      return this.self();
   }

   @Override
   public S name(String name) {
      attributes.attribute(NAME).set(name);
      return this.self();
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public S host(String host) {
      attributes.attribute(HOST).set(host);
      return this.self();
   }

   @Override
   public S port(int port) {
      attributes.attribute(PORT).set(port);
      return this.self();
   }

   @Override
   public S idleTimeout(int idleTimeout) {
      attributes.attribute(IDLE_TIMEOUT).set(idleTimeout);
      return this.self();
   }

   @Override
   public S tcpNoDelay(boolean tcpNoDelay) {
      attributes.attribute(TCP_NODELAY).set(tcpNoDelay);
      return this.self();
   }

   @Override
   public S tcpKeepAlive(boolean tcpKeepAlive) {
      attributes.attribute(TCP_KEEPALIVE).set(tcpKeepAlive);
      return this.self();
   }

   @Override
   public S recvBufSize(int recvBufSize) {
      attributes.attribute(RECV_BUF_SIZE).set(recvBufSize);
      return this.self();
   }

   @Override
   public S sendBufSize(int sendBufSize) {
      attributes.attribute(SEND_BUF_SIZE).set(sendBufSize);
      return this.self();
   }

   @Override
   public SslConfigurationBuilder ssl() {
      return ssl;
   }

   @Override
   public S ioThreads(int ioThreads) {
      attributes.attribute(IO_THREADS).set(ioThreads);
      return this.self();
   }

   @Override
   public S workerThreads(int workerThreads) {
      attributes.attribute(WORKER_THREADS).set(workerThreads);
      return this.self();
   }

   @Override
   public S startTransport(boolean startTransport) {
      attributes.attribute(START_TRANSPORT).set(startTransport);
      return this.self();
   }

   @Override
   public S adminOperationsHandler(AdminOperationsHandler handler) {
      attributes.attribute(ADMIN_OPERATION_HANDLER).set(handler);
      return this.self();
   }


   @Override
   public void validate() {
      ssl.validate();
      if (attributes.attribute(IDLE_TIMEOUT).get() < -1) {
         throw log.illegalIdleTimeout(attributes.attribute(IDLE_TIMEOUT).get());
      }
      if (attributes.attribute(SEND_BUF_SIZE).get() < 0) {
         throw log.illegalSendBufferSize(attributes.attribute(SEND_BUF_SIZE).get());
      }
      if (attributes.attribute(RECV_BUF_SIZE).get() < 0) {
         throw log.illegalReceiveBufferSize(attributes.attribute(RECV_BUF_SIZE).get());
      }
      if (attributes.attribute(IO_THREADS).get() < 0) {
         throw log.illegalIOThreads(attributes.attribute(IO_THREADS).get());
      }
      if (attributes.attribute(WORKER_THREADS).get() < 0) {
         throw log.illegalWorkerThreads(attributes.attribute(WORKER_THREADS).get());
      }
   }

   @Override
   public Builder<?> read(T template) {
      this.attributes.read(template.attributes());
      this.ssl.read(template.ssl());
      return this;
   }
}

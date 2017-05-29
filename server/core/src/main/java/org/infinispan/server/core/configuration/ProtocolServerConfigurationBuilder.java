package org.infinispan.server.core.configuration;

import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.logging.LogFactory;

public abstract class ProtocolServerConfigurationBuilder<T extends ProtocolServerConfiguration, S extends ProtocolServerConfigurationChildBuilder<T, S>>
      implements ProtocolServerConfigurationChildBuilder<T, S>, Builder<T> {
   private static final Log log = LogFactory.getLog(ProtocolServerConfigurationBuilder.class, Log.class);
   protected String defaultCacheName = BasicCacheContainer.DEFAULT_CACHE_NAME;
   protected String name = "";
   protected String host = "127.0.0.1";
   protected int port = -1;
   protected int idleTimeout = -1;
   protected int recvBufSize = 0;
   protected int sendBufSize = 0;
   protected final SslConfigurationBuilder<T, S> ssl;
   protected boolean tcpNoDelay = true;
   protected int workerThreads = 2 * Runtime.getRuntime().availableProcessors();
   protected Set<String> ignoredCaches = Collections.EMPTY_SET;
   protected boolean startTransport = true;
   protected AdminOperationsHandler adminOperationsHandler;

   protected ProtocolServerConfigurationBuilder(int port) {
      this.port = port;
      this.ssl = new SslConfigurationBuilder(this);
   }

   @Override
   public S ignoredCaches(Set<String> ignoredCaches) {
      this.ignoredCaches = ignoredCaches;
      return this.self();
   }

   @Override
   public S defaultCacheName(String defaultCacheName) {
      this.defaultCacheName = defaultCacheName;
      return this.self();
   }

   @Override
   public S name(String name) {
      this.name = name;
      return this.self();
   }

   public String name() {
      return name;
   }

   @Override
   public S host(String host) {
      this.host = host;
      return this.self();
   }

   @Override
   public S port(int port) {
      this.port = port;
      return this.self();
   }

   @Override
   public S idleTimeout(int idleTimeout) {
      this.idleTimeout = idleTimeout;
      return this.self();
   }

   @Override
   public S tcpNoDelay(boolean tcpNoDelay) {
      this.tcpNoDelay = tcpNoDelay;
      return this.self();
   }

   @Override
   public S recvBufSize(int recvBufSize) {
      this.recvBufSize = recvBufSize;
      return this.self();
   }

   @Override
   public S sendBufSize(int sendBufSize) {
      this.sendBufSize = sendBufSize;
      return this.self();
   }

   @Override
   public SslConfigurationBuilder ssl() {
      return ssl;
   }

   @Override
   public S workerThreads(int workerThreads) {
      this.workerThreads = workerThreads;
      return this.self();
   }

   @Override
   public S startTransport(boolean startTransport) {
      this.startTransport = startTransport;
      return this.self();
   }

   @Override
   public S adminOperationsHandler(AdminOperationsHandler handler) {
      this.adminOperationsHandler = handler;
      return this.self();
   }


   @Override
   public void validate() {
      ssl.validate();
      if (idleTimeout < -1) {
         throw log.illegalIdleTimeout(idleTimeout);
      }
      if (sendBufSize < 0) {
         throw log.illegalSendBufferSize(sendBufSize);
      }
      if (recvBufSize < 0) {
         throw log.illegalReceiveBufferSize(recvBufSize);
      }
      if (workerThreads < 0) {
         throw log.illegalWorkerThreads(workerThreads);
      }
   }

   @Override
   public Builder<?> read(T template) {
      this.defaultCacheName = template.defaultCacheName();
      this.name = template.name();
      this.host = template.host();
      this.port = template.port();
      this.idleTimeout = template.idleTimeout();
      this.recvBufSize = template.recvBufSize();
      this.sendBufSize = template.sendBufSize();
      this.tcpNoDelay = template.tcpNoDelay();
      this.workerThreads = template.workerThreads();
      this.ssl.read(template.ssl());
      this.ignoredCaches = template.ignoredCaches();
      this.startTransport = template.startTransport();
      this.adminOperationsHandler = template.adminOperationsHandler();
      return this;
   }
}

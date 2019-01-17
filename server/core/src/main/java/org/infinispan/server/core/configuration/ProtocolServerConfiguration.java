package org.infinispan.server.core.configuration;

import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.server.core.admin.AdminOperationsHandler;

/**
 * ServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public abstract class ProtocolServerConfiguration {
   public static final AttributeDefinition<String> DEFAULT_CACHE_NAME = AttributeDefinition.builder("default-cache", BasicCacheContainer.DEFAULT_CACHE_NAME).immutable().build();
   public static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", "").immutable().build();
   public static final AttributeDefinition<String> HOST = AttributeDefinition.builder("host", "127.0.0.1").immutable().build();
   public static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder("port", -1).immutable().build();
   public static final AttributeDefinition<Integer> IDLE_TIMEOUT = AttributeDefinition.builder("idle-timeout", -1).immutable().build();
   public static final AttributeDefinition<Set<String>> IGNORED_CACHES = AttributeDefinition.builder("ignored-caches", Collections.emptySet(), (Class<Set<String>>) (Class<?>) Set.class).immutable().build();
   public static final AttributeDefinition<Integer> RECV_BUF_SIZE = AttributeDefinition.builder("recv-buffer-size", 0).immutable().build();
   public static final AttributeDefinition<Integer> SEND_BUF_SIZE = AttributeDefinition.builder("send-buffer-size", 0).immutable().build();
   public static final AttributeDefinition<Boolean> START_TRANSPORT = AttributeDefinition.builder("start-transport", true).immutable().build();
   public static final AttributeDefinition<Boolean> TCP_NODELAY = AttributeDefinition.builder("tcp-nodelay", true).immutable().build();
   public static final AttributeDefinition<Boolean> TCP_KEEPALIVE = AttributeDefinition.builder("tcp-keepalive", false).immutable().build();
   public static final AttributeDefinition<Integer> IO_THREADS = AttributeDefinition.builder("io-threads", 2 * ProcessorInfo.availableProcessors()).immutable().build();
   public static final AttributeDefinition<Integer> WORKER_THREADS = AttributeDefinition.builder("worker-threads", 160).immutable().build();
   public static final AttributeDefinition<AdminOperationsHandler> ADMIN_OPERATION_HANDLER = AttributeDefinition.builder("admin-operation-handler", null, AdminOperationsHandler.class).immutable().build();
   public static final AttributeDefinition<Boolean> ZERO_CAPACITY_NODE = AttributeDefinition.builder("zero-capacity-node", false).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ProtocolServerConfiguration.class,
            DEFAULT_CACHE_NAME, NAME, HOST, PORT, IDLE_TIMEOUT, IGNORED_CACHES, RECV_BUF_SIZE, SEND_BUF_SIZE, START_TRANSPORT, TCP_NODELAY, TCP_KEEPALIVE, IO_THREADS, WORKER_THREADS, ADMIN_OPERATION_HANDLER, ZERO_CAPACITY_NODE);
   }

   private final Attribute<String> defaultCacheName;
   private final Attribute<String> name;
   private final Attribute<String> host;
   private final Attribute<Integer> port;
   private final Attribute<Integer> idleTimeout;
   private final Attribute<Integer> recvBufSize;
   private final Attribute<Integer> sendBufSize;
   private final Attribute<Boolean> tcpNoDelay;
   private final Attribute<Boolean> tcpKeepAlive;
   private final Attribute<Integer> ioThreads;
   private final Attribute<Integer> workerThreads;
   private final Attribute<Set<String>> ignoredCaches;
   private final Attribute<Boolean> startTransport;
   private final Attribute<AdminOperationsHandler> adminOperationsHandler;
   private final Attribute<Boolean> zeroCapacityNode;

   protected final AttributeSet attributes;

   protected final SslConfiguration ssl;

   protected ProtocolServerConfiguration(AttributeSet attributes, SslConfiguration ssl) {
      this.attributes = attributes.checkProtection();
      this.ssl = ssl;

      defaultCacheName = attributes.attribute(DEFAULT_CACHE_NAME);
      zeroCapacityNode = attributes.attribute(ZERO_CAPACITY_NODE);
      name = attributes.attribute(NAME);
      host = attributes.attribute(HOST);
      port = attributes.attribute(PORT);
      idleTimeout = attributes.attribute(IDLE_TIMEOUT);
      ignoredCaches = attributes.attribute(IGNORED_CACHES);
      recvBufSize = attributes.attribute(RECV_BUF_SIZE);
      sendBufSize = attributes.attribute(SEND_BUF_SIZE);
      startTransport = attributes.attribute(START_TRANSPORT);
      tcpNoDelay = attributes.attribute(TCP_NODELAY);
      tcpKeepAlive = attributes.attribute(TCP_KEEPALIVE);
      ioThreads = attributes.attribute(IO_THREADS);
      workerThreads = attributes.attribute(WORKER_THREADS);
      adminOperationsHandler = attributes.attribute(ADMIN_OPERATION_HANDLER);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String defaultCacheName() {
      return defaultCacheName.get();
   }

   public String name() {
      return name.get();
   }

   public String host() {
      return host.get();
   }

   public int port() {
      return port.get();
   }

   public int idleTimeout() {
      return idleTimeout.get();
   }

   public int recvBufSize() {
      return recvBufSize.get();
   }

   public int sendBufSize() {
      return sendBufSize.get();
   }

   public SslConfiguration ssl() {
      return ssl;
   }

   public boolean tcpNoDelay() {
      return tcpNoDelay.get();
   }

   public boolean tcpKeepAlive() {
      return tcpKeepAlive.get();
   }

   public int ioThreads() {
      return ioThreads.get();
   }

   public int workerThreads() {
      return workerThreads.get();
   }

   public Set<String> ignoredCaches() {
      return ignoredCaches.get();
   }

   public boolean startTransport() {
      return startTransport.get();
   }

   public AdminOperationsHandler adminOperationsHandler() {
      return adminOperationsHandler.get();
   }

   public boolean zeroCapacityNode() {
      return zeroCapacityNode.get();
   }

   @Override
   public String toString() {
      return "ProtocolServerConfiguration[" + attributes + "]";
   }
}

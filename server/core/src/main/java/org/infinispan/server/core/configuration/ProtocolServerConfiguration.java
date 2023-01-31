package org.infinispan.server.core.configuration;

import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.server.core.admin.AdminOperationsHandler;

/**
 * ServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public abstract class ProtocolServerConfiguration<T extends ProtocolServerConfiguration, A extends AuthenticationConfiguration> extends ConfigurationElement<T> {
   public static final AttributeDefinition<String> DEFAULT_CACHE_NAME = AttributeDefinition.builder(Attribute.CACHE, null, String.class).immutable().build();
   public static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, "").immutable().build();
   public static final AttributeDefinition<String> HOST = AttributeDefinition.builder(Attribute.HOST, "127.0.0.1").immutable().autoPersist(false).build();
   public static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder(Attribute.PORT, -1).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Integer> IDLE_TIMEOUT = AttributeDefinition.builder(Attribute.IDLE_TIMEOUT, -1).immutable().build();
   public static final AttributeDefinition<Set<String>> IGNORED_CACHES = AttributeDefinition.builder(Attribute.IGNORED_CACHES, Collections.emptySet(), (Class<Set<String>>) (Class<?>) Set.class).immutable().build();
   public static final AttributeDefinition<Integer> RECV_BUF_SIZE = AttributeDefinition.builder(Attribute.RECEIVE_BUFFER_SIZE, 0).immutable().build();
   public static final AttributeDefinition<Integer> SEND_BUF_SIZE = AttributeDefinition.builder(Attribute.SEND_BUFFER_SIZE, 0).immutable().build();
   public static final AttributeDefinition<Boolean> START_TRANSPORT = AttributeDefinition.builder(Attribute.START_TRANSPORT, true).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Boolean> TCP_NODELAY = AttributeDefinition.builder(Attribute.TCP_NODELAY, true).immutable().build();
   public static final AttributeDefinition<Boolean> TCP_KEEPALIVE = AttributeDefinition.builder(Attribute.TCP_KEEPALIVE, false).immutable().build();
   public static final AttributeDefinition<Integer> IO_THREADS = AttributeDefinition.builder(Attribute.IO_THREADS, 2 * ProcessorInfo.availableProcessors()).immutable().build();
   public static final AttributeDefinition<AdminOperationsHandler> ADMIN_OPERATION_HANDLER = AttributeDefinition.builder("admin-operation-handler", null, AdminOperationsHandler.class)
         .immutable().autoPersist(false).build();
   public static final AttributeDefinition<Boolean> ZERO_CAPACITY_NODE = AttributeDefinition.builder(Attribute.ZERO_CAPACITY_NODE, false).immutable().build();
   public static final AttributeDefinition<String> SOCKET_BINDING = AttributeDefinition.builder(Attribute.SOCKET_BINDING, null, String.class).immutable().build();
   public static final AttributeDefinition<Boolean> IMPLICIT_CONNECTOR = AttributeDefinition.builder("implicit-connector", false).immutable().autoPersist(false).build();

   private volatile boolean enabled = true;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ProtocolServerConfiguration.class,
            DEFAULT_CACHE_NAME, NAME, HOST, PORT, IDLE_TIMEOUT, IGNORED_CACHES, RECV_BUF_SIZE, SEND_BUF_SIZE, START_TRANSPORT,
            TCP_NODELAY, TCP_KEEPALIVE, IO_THREADS, ADMIN_OPERATION_HANDLER, ZERO_CAPACITY_NODE, SOCKET_BINDING,
            IMPLICIT_CONNECTOR);
   }

   protected final A authentication;
   protected final SslConfiguration ssl;
   protected final IpFilterConfiguration ipFilter;

   protected ProtocolServerConfiguration(Enum<?> element, AttributeSet attributes, A authentication, SslConfiguration ssl, IpFilterConfiguration ipFilter) {
      this(element.toString(), attributes, authentication, ssl, ipFilter);
   }

   protected ProtocolServerConfiguration(String element, AttributeSet attributes, A authentication, SslConfiguration ssl, IpFilterConfiguration ipFilter) {
      super(element, attributes, ssl);
      this.authentication = authentication;
      this.ssl = ssl;
      this.ipFilter = ipFilter;
   }

   public String defaultCacheName() {
      return attributes.attribute(DEFAULT_CACHE_NAME).get();
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String host() {
      return attributes.attribute(HOST).get();
   }

   public int port() {
      return attributes.attribute(PORT).get();
   }

   public int idleTimeout() {
      return attributes.attribute(IDLE_TIMEOUT).get();
   }

   public int recvBufSize() {
      return attributes.attribute(RECV_BUF_SIZE).get();
   }

   public int sendBufSize() {
      return attributes.attribute(SEND_BUF_SIZE).get();
   }

   public A authentication() {
      return authentication;
   }

   public SslConfiguration ssl() {
      return ssl;
   }

   public IpFilterConfiguration ipFilter() {
      return ipFilter;
   }

   public boolean tcpNoDelay() {
      return attributes.attribute(TCP_NODELAY).get();
   }

   public boolean tcpKeepAlive() {
      return attributes.attribute(TCP_KEEPALIVE).get();
   }

   public int ioThreads() {
      return attributes.attribute(IO_THREADS).get();
   }

   public boolean startTransport() {
      return attributes.attribute(START_TRANSPORT).get();
   }

   public AdminOperationsHandler adminOperationsHandler() {
      return attributes.attribute(ADMIN_OPERATION_HANDLER).get();
   }

   public String socketBinding() {
      return attributes.attribute(SOCKET_BINDING).get();
   }

   public boolean zeroCapacityNode() {
      return attributes.attribute(ZERO_CAPACITY_NODE).get();
   }

   public void disable() {
      this.enabled = false;
   }

   public void enable() {
      this.enabled = true;
   }

   public boolean isEnabled() {
      return enabled;
   }

   public boolean isImplicit() {
      return attributes.attribute(IMPLICIT_CONNECTOR).get();
   }
}

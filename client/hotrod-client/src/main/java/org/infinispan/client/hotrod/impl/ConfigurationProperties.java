/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.client.hotrod.impl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.marshall.jboss.GenericJBossMarshaller;

/**
 * Encapsulate all config properties here
 *
 * @author Manik Surtani
 * @version 4.1
 */
public class ConfigurationProperties {
   public static final String TRANSPORT_FACTORY = "infinispan.client.hotrod.transport_factory";
   public static final String SERVER_LIST = "infinispan.client.hotrod.server_list";
   public static final String MARSHALLER = "infinispan.client.hotrod.marshaller";
   public static final String ASYNC_EXECUTOR_FACTORY = "infinispan.client.hotrod.async_executor_factory";
   public static final String DEFAULT_EXECUTOR_FACTORY_POOL_SIZE = "infinispan.client.hotrod.default_executor_factory.pool_size";
   public static final String TCP_NO_DELAY = "infinispan.client.hotrod.tcp_no_delay";
   public static final String PING_ON_STARTUP = "infinispan.client.hotrod.ping_on_startup";
   public static final String REQUEST_BALANCING_STRATEGY = "infinispan.client.hotrod.request_balancing_strategy";
   public static final String KEY_SIZE_ESTIMATE = "infinispan.client.hotrod.key_size_estimate";
   public static final String VALUE_SIZE_ESTIMATE = "infinispan.client.hotrod.value_size_estimate";
   public static final String FORCE_RETURN_VALUES = "infinispan.client.hotrod.force_return_values";
   public static final String HASH_FUNCTION_PREFIX = "infinispan.client.hotrod.hash_function_impl";
   public static final String DEFAULT_EXECUTOR_FACTORY_QUEUE_SIZE = "infinispan.client.hotrod.default_executor_factory.queue_size";
   public static final String SO_TIMEOUT = "infinispan.client.hotrod.socket_timeout";
   public static final String CONNECT_TIMEOUT = "infinispan.client.hotrod.connect_timeout";
   public static final String PROTOCOL_VERSION = "infinispan.client.hotrod.protocol_version";
   public static final String USE_SSL = "infinispan.client.hotrod.use_ssl";
   public static final String KEY_STORE_FILE_NAME = "infinispan.client.hotrod.key_store_file_name";
   public static final String KEY_STORE_PASSWORD = "infinispan.client.hotrod.key_store_password";
   public static final String TRUST_STORE_FILE_NAME = "infinispan.client.hotrod.trust_store_file_name";
   public static final String TRUST_STORE_PASSWORD = "infinispan.client.hotrod.trust_store_password";

   // defaults

   public static final int DEFAULT_KEY_SIZE = 64;
   public static final int DEFAULT_VALUE_SIZE = 512;
   public static final int DEFAULT_HOTROD_PORT = 11222;
   public static final int DEFAULT_SO_TIMEOUT = 60000;
   public static final int DEFAULT_CONNECT_TIMEOUT = 60000;
   public static final String PROTOCOL_VERSION_12 = "1.2";
   public static final String PROTOCOL_VERSION_11 = "1.1";
   public static final String PROTOCOL_VERSION_10 = "1.0";
   public static final String DEFAULT_PROTOCOL_VERSION = PROTOCOL_VERSION_12;

   private final TypedProperties props;


   public ConfigurationProperties() {
      this.props = new TypedProperties();
   }

   public ConfigurationProperties(String serverList) {
      this();
      props.setProperty(SERVER_LIST, serverList);
   }

   public ConfigurationProperties(Properties props) {
      this.props = props == null ? new TypedProperties() : TypedProperties.toTypedProperties(props);
   }

   public String getTransportFactory() {
      return props.getProperty(TRANSPORT_FACTORY, TcpTransportFactory.class.getName());
   }

   public Collection<SocketAddress> getServerList() {
      Set<SocketAddress> addresses = new HashSet<SocketAddress>();
      String servers = props.getProperty(SERVER_LIST, "127.0.0.1:" + DEFAULT_HOTROD_PORT);
      for (String server : servers.split(";")) {
         String[] components = server.trim().split(":");
         String host = components[0];
         int port = DEFAULT_HOTROD_PORT;
         if (components.length > 1) port = Integer.parseInt(components[1]);
         addresses.add(new InetSocketAddress(host, port));
      }

      if (addresses.isEmpty()) throw new IllegalStateException("No Hot Rod servers specified!");

      return addresses;
   }

   public String getMarshaller() {
      return props.getProperty(MARSHALLER, GenericJBossMarshaller.class.getName());
   }

   public String getAsyncExecutorFactory() {
      return props.getProperty(ASYNC_EXECUTOR_FACTORY, DefaultAsyncExecutorFactory.class.getName());
   }

   public int getDefaultExecutorFactoryPoolSize() {
      return props.getIntProperty(DEFAULT_EXECUTOR_FACTORY_POOL_SIZE, 99);
   }

   public int getDefaultExecutorFactoryQueueSize() {
      return props.getIntProperty(DEFAULT_EXECUTOR_FACTORY_QUEUE_SIZE, 10000);
   }

   public boolean getTcpNoDelay() {
      return props.getBooleanProperty(TCP_NO_DELAY, true);
   }

   public boolean getPingOnStartup() {
      return props.getBooleanProperty(PING_ON_STARTUP, true);
   }

   public String getRequestBalancingStrategy() {
      return props.getProperty(REQUEST_BALANCING_STRATEGY, RoundRobinBalancingStrategy.class.getName());
   }

   public int getKeySizeEstimate() {
      return props.getIntProperty(KEY_SIZE_ESTIMATE, DEFAULT_KEY_SIZE);
   }

   public int getValueSizeEstimate() {
      return props.getIntProperty(VALUE_SIZE_ESTIMATE, DEFAULT_VALUE_SIZE);
   }

   public boolean getForceReturnValues() {
      return props.getBooleanProperty(FORCE_RETURN_VALUES, false);
   }

   public Properties getProperties() {
      return props;
   }

   public int getSoTimeout() {
      return props.getIntProperty(SO_TIMEOUT, DEFAULT_SO_TIMEOUT);
   }

   public String getProtocolVersion() {
      return props.getProperty(PROTOCOL_VERSION, DEFAULT_PROTOCOL_VERSION);
   }

   public int getConnectTimeout() {
      return props.getIntProperty(CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
   }

   public boolean getUseSSL() {
      return props.getBooleanProperty(USE_SSL, false);
   }

   public String getKeyStoreFileName() {
      return props.getProperty(KEY_STORE_FILE_NAME, null);
   }

   public String getKeyStorePassword() {
      return props.getProperty(KEY_STORE_PASSWORD, null);
   }

   public String getTrustStoreFileName() {
      return props.getProperty(TRUST_STORE_FILE_NAME, null);
   }

   public String getTrustStorePassword() {
      return props.getProperty(TRUST_STORE_PASSWORD, null);
   }

}

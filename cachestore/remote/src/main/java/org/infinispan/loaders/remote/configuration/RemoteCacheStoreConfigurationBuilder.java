/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.remote.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.api.BasicCacheContainer;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.remote.RemoteCacheStore;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.TypedProperties;

/**
 * RemoteCacheStoreConfigurationBuilder. Configures a {@link RemoteCacheStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class RemoteCacheStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<RemoteCacheStoreConfiguration, RemoteCacheStoreConfigurationBuilder> implements
      RemoteCacheStoreConfigurationChildBuilder<RemoteCacheStoreConfigurationBuilder> {
   private final ExecutorFactoryConfigurationBuilder asyncExecutorFactory;
   private String balancingStrategy = RoundRobinBalancingStrategy.class.getName();
   private final ConnectionPoolConfigurationBuilder connectionPool;
   private long connectionTimeout = ConfigurationProperties.DEFAULT_CONNECT_TIMEOUT;
   private boolean forceReturnValues;
   private int keySizeEstimate = ConfigurationProperties.DEFAULT_KEY_SIZE;
   private String marshaller;
   private boolean pingOnStartup = true;
   private String protocolVersion;
   private String remoteCacheName = BasicCacheContainer.DEFAULT_CACHE_NAME;
   private List<RemoteServerConfigurationBuilder> servers = new ArrayList<RemoteServerConfigurationBuilder>();
   private long socketTimeout = ConfigurationProperties.DEFAULT_SO_TIMEOUT;
   private boolean tcpNoDelay = true;
   private String transportFactory;
   private int valueSizeEstimate = ConfigurationProperties.DEFAULT_VALUE_SIZE;

   public RemoteCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      asyncExecutorFactory = new ExecutorFactoryConfigurationBuilder(this);
      connectionPool = new ConnectionPoolConfigurationBuilder(this);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncExecutorFactory() {
      return asyncExecutorFactory;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder balancingStrategy(String balancingStrategy) {
      this.balancingStrategy = balancingStrategy;
      return this;
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return connectionPool;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder connectionTimeout(long connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      this.forceReturnValues = forceReturnValues;
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder keySizeEstimate(int keySizeEstimate) {
      this.keySizeEstimate = keySizeEstimate;
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder marshaller(String marshaller) {
      this.marshaller = marshaller;
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller) {
      this.marshaller = marshaller.getName();
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder pingOnStartup(boolean pingOnStartup) {
      this.pingOnStartup = pingOnStartup;
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder protocolVersion(String protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder remoteCacheName(String remoteCacheName) {
      this.remoteCacheName = remoteCacheName;
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder socketTimeout(long socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      this.tcpNoDelay = tcpNoDelay;
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder transportFactory(String transportFactory) {
      this.transportFactory = transportFactory;
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder transportFactory(Class<? extends TransportFactory> transportFactory) {
      this.transportFactory = transportFactory.getName();
      return this;
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder valueSizeEstimate(int valueSizeEstimate) {
      this.valueSizeEstimate = valueSizeEstimate;
      return this;
   }

   @Override
   public RemoteServerConfigurationBuilder addServer() {
      RemoteServerConfigurationBuilder builder = new RemoteServerConfigurationBuilder(this);
      this.servers.add(builder);
      return builder;
   }

   @Override
   public RemoteCacheStoreConfiguration create() {
      List<RemoteServerConfiguration> remoteServers = new ArrayList<RemoteServerConfiguration>();
      for (RemoteServerConfigurationBuilder server : servers) {
         remoteServers.add(server.create());
      }
      return new RemoteCacheStoreConfiguration(asyncExecutorFactory.create(), balancingStrategy,
            connectionPool.create(), connectionTimeout, forceReturnValues, keySizeEstimate, marshaller, pingOnStartup,
            protocolVersion, remoteCacheName, remoteServers, socketTimeout, tcpNoDelay, transportFactory,
            valueSizeEstimate, purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState,
            ignoreModifications, TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder read(RemoteCacheStoreConfiguration template) {
      this.asyncExecutorFactory.read(template.asyncExecutorFactory());
      this.balancingStrategy = template.balancingStrategy();
      this.connectionPool.read(template.connectionPool());
      this.connectionTimeout = template.connectionTimeout();
      this.forceReturnValues = template.forceReturnValues();
      this.keySizeEstimate = template.keySizeEstimate();
      this.marshaller = template.marshaller();
      this.pingOnStartup = template.pingOnStartup();
      this.protocolVersion = template.protocolVersion();
      this.remoteCacheName = template.remoteCacheName();
      this.socketTimeout = template.socketTimeout();
      this.tcpNoDelay = template.tcpNoDelay();
      this.transportFactory = template.transportFactory();
      this.valueSizeEstimate = template.valueSizeEstimate();
      for(RemoteServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }

      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      async.read(template.async());
      singletonStore.read(template.singletonStore());
      return this;
   }

}

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

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.configuration.BuiltBy;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.remote.RemoteCacheStoreConfig;
import org.infinispan.util.TypedProperties;

@BuiltBy(RemoteCacheStoreConfigurationBuilder.class)
public class RemoteCacheStoreConfiguration extends AbstractStoreConfiguration implements
      LegacyLoaderAdapter<RemoteCacheStoreConfig> {

   private final ExecutorFactoryConfiguration asyncExecutorFactory;
   private final String balancingStrategy;
   private final ConnectionPoolConfiguration connectionPool;
   private final long connectionTimeout;
   private final boolean forceReturnValues;
   private final int keySizeEstimate;
   private final String marshaller;
   private final boolean pingOnStartup;
   private final String protocolVersion;
   private final String remoteCacheName;
   private final List<RemoteServerConfiguration> servers;
   private final long socketTimeout;
   private final boolean tcpNoDelay;
   private final String transportFactory;
   private final int valueSizeEstimate;

   RemoteCacheStoreConfiguration(ExecutorFactoryConfiguration asyncExecutorFactory, String balancingStrategy,
         ConnectionPoolConfiguration connectionPool, long connectionTimeout, boolean forceReturnValues,
         int keySizeEstimate, String marshaller, boolean pingOnStartup, String protocolVersion, String remoteCacheName,
         List<RemoteServerConfiguration> servers, long socketTimeout, boolean tcpNoDelay, String transportFactory,
         int valueSizeEstimate, boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads,
         boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties,
         AsyncStoreConfiguration asyncStoreConfiguration, SingletonStoreConfiguration singletonStoreConfiguration) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties,
            asyncStoreConfiguration, singletonStoreConfiguration);
      this.asyncExecutorFactory = asyncExecutorFactory;
      this.balancingStrategy = balancingStrategy;
      this.connectionPool = connectionPool;
      this.connectionTimeout = connectionTimeout;
      this.forceReturnValues = forceReturnValues;
      this.keySizeEstimate = keySizeEstimate;
      this.marshaller = marshaller;
      this.pingOnStartup = pingOnStartup;
      this.protocolVersion = protocolVersion;
      this.remoteCacheName = remoteCacheName;
      this.servers = Collections.unmodifiableList(servers);
      this.socketTimeout = socketTimeout;
      this.tcpNoDelay = tcpNoDelay;
      this.transportFactory = transportFactory;
      this.valueSizeEstimate = valueSizeEstimate;
   }

   public ExecutorFactoryConfiguration asyncExecutorFactory() {
      return asyncExecutorFactory;
   }

   public String balancingStrategy() {
      return balancingStrategy;
   }

   public ConnectionPoolConfiguration connectionPool() {
      return connectionPool;
   }

   public long connectionTimeout() {
      return connectionTimeout;
   }

   public boolean forceReturnValues() {
      return forceReturnValues;
   }

   public int keySizeEstimate() {
      return keySizeEstimate;
   }

   public String marshaller() {
      return marshaller;
   }

   public boolean pingOnStartup() {
      return pingOnStartup;
   }

   public String protocolVersion() {
      return protocolVersion;
   }

   public String remoteCacheName() {
      return remoteCacheName;
   }

   public List<RemoteServerConfiguration> servers() {
      return servers;
   }

   public long socketTimeout() {
      return socketTimeout;
   }

   public boolean tcpNoDelay() {
      return tcpNoDelay;
   }

   public String transportFactory() {
      return transportFactory;
   }

   public int valueSizeEstimate() {
      return valueSizeEstimate;
   }

   @Override
   public RemoteCacheStoreConfig adapt() {
      RemoteCacheStoreConfig config = new RemoteCacheStoreConfig();
      // StoreConfiguration
      config.fetchPersistentState(fetchPersistentState());
      config.ignoreModifications(ignoreModifications());
      config.purgeOnStartup(purgeOnStartup());
      config.purgeSynchronously(purgeSynchronously());
      config.purgerThreads(purgerThreads());

      // RemoteCacheStoreConfiguration
      config.setRemoteCacheName(remoteCacheName);
      config.setAsyncExecutorFactory(asyncExecutorFactory.factory());

      TypedProperties p = new TypedProperties();

      // Async Executor
      p.putAll(asyncExecutorFactory.properties());

      // Connection Pool
      p.put("maxActive", Integer.toString(connectionPool.maxActive()));
      p.put("maxIdle", Integer.toString(connectionPool.maxIdle()));
      p.put("maxTotal", Integer.toString(connectionPool.maxTotal()));
      p.put("minIdle", connectionPool.minIdle());
      p.put("minEvictableIdleTimeMillis", Long.toString(connectionPool.minEvictableIdleTime()));
      p.put("testWhileIdle", Boolean.toString(connectionPool.testWhileIdle()));
      p.put("timeBetweenEvictionRunsMillis", Long.toString(connectionPool.timeBetweenEvictionRuns()));
      p.put("whenExhaustedAction", Integer.toString(connectionPool.exhaustedAction().ordinal()));

      config.setTypedProperties(p);

      Properties hrp = new Properties();
      hrp.put(ConfigurationProperties.CONNECT_TIMEOUT, Long.toString(connectionTimeout));
      hrp.put(ConfigurationProperties.FORCE_RETURN_VALUES, Boolean.toString(forceReturnValues));
      hrp.put(ConfigurationProperties.KEY_SIZE_ESTIMATE, Integer.toString(keySizeEstimate));
      hrp.put(ConfigurationProperties.PING_ON_STARTUP, Boolean.toString(pingOnStartup));
      StringBuilder serverList = new StringBuilder();
      for (RemoteServerConfiguration server : servers) {
         if (serverList.length() > 0)
            serverList.append(";");
         serverList.append(server.host());
         serverList.append(":");
         serverList.append(server.port());
      }
      hrp.put(ConfigurationProperties.SERVER_LIST, serverList.toString());
      hrp.put(ConfigurationProperties.SO_TIMEOUT, Long.toString(socketTimeout));
      hrp.put(ConfigurationProperties.TCP_NO_DELAY, Boolean.toString(tcpNoDelay));
      hrp.put(ConfigurationProperties.VALUE_SIZE_ESTIMATE, Integer.toString(valueSizeEstimate));
      if (marshaller != null)
         hrp.put(ConfigurationProperties.MARSHALLER, marshaller);
      if (transportFactory != null)
         hrp.put(ConfigurationProperties.TRANSPORT_FACTORY, transportFactory);

      config.setHotRodClientProperties(hrp);
      return config;
   }

}

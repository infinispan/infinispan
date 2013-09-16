package org.infinispan.persistence.remote.configuration;

import java.util.List;
import java.util.Properties;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.remote.RemoteStore;

@BuiltBy(RemoteStoreConfigurationBuilder.class)
@ConfigurationFor(RemoteStore.class)
public class RemoteStoreConfiguration extends AbstractStoreConfiguration {

   private final ExecutorFactoryConfiguration asyncExecutorFactory;
   private final String balancingStrategy;
   private final ConnectionPoolConfiguration connectionPool;
   private final long connectionTimeout;
   private final boolean forceReturnValues;
   private final boolean hotRodWrapping;
   private final int keySizeEstimate;
   private final String marshaller;
   private final boolean pingOnStartup;
   private final String protocolVersion;
   private final boolean rawValues;
   private final String remoteCacheName;
   private final List<RemoteServerConfiguration> servers;
   private final long socketTimeout;
   private final boolean tcpNoDelay;
   private final String transportFactory;
   private final int valueSizeEstimate;

   public RemoteStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                   AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                   boolean preload, boolean shared, Properties properties,
                                   ExecutorFactoryConfiguration asyncExecutorFactory, String balancingStrategy,
                                   ConnectionPoolConfiguration connectionPool, long connectionTimeout,
                                   boolean forceReturnValues, boolean hotRodWrapping, int keySizeEstimate,
                                   String marshaller, boolean pingOnStartup, String protocolVersion,
                                   boolean rawValues, String remoteCacheName,
                                   List<RemoteServerConfiguration> servers, long socketTimeout,
                                   boolean tcpNoDelay, String transportFactory, int valueSizeEstimate) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.asyncExecutorFactory = asyncExecutorFactory;
      this.balancingStrategy = balancingStrategy;
      this.connectionPool = connectionPool;
      this.connectionTimeout = connectionTimeout;
      this.forceReturnValues = forceReturnValues;
      this.hotRodWrapping = hotRodWrapping;
      this.keySizeEstimate = keySizeEstimate;
      this.marshaller = marshaller;
      this.pingOnStartup = pingOnStartup;
      this.protocolVersion = protocolVersion;
      this.rawValues = rawValues;
      this.remoteCacheName = remoteCacheName;
      this.servers = servers;
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

   public boolean hotRodWrapping() {
      return hotRodWrapping;
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

   public boolean rawValues() {
      return rawValues;
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
}


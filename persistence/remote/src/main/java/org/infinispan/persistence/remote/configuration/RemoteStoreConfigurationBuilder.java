package org.infinispan.persistence.remote.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.util.logging.LogFactory;

/**
 * RemoteStoreConfigurationBuilder. Configures a {@link org.infinispan.persistence.remote.RemoteStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class RemoteStoreConfigurationBuilder extends
                                                  AbstractStoreConfigurationBuilder<RemoteStoreConfiguration, RemoteStoreConfigurationBuilder> implements
                                                                                                                                               RemoteStoreConfigurationChildBuilder<RemoteStoreConfigurationBuilder> {
   private static final Log log = LogFactory.getLog(RemoteStoreConfigurationBuilder.class, Log.class);
   private final ExecutorFactoryConfigurationBuilder asyncExecutorFactory;
   private String balancingStrategy = RoundRobinBalancingStrategy.class.getName();
   private final ConnectionPoolConfigurationBuilder connectionPool;
   private long connectionTimeout = ConfigurationProperties.DEFAULT_CONNECT_TIMEOUT;
   private boolean forceReturnValues;
   private boolean hotRodWrapping;
   private int keySizeEstimate = ConfigurationProperties.DEFAULT_KEY_SIZE;
   private String marshaller;
   private boolean pingOnStartup = true;
   private String protocolVersion;
   private boolean rawValues;
   private String remoteCacheName = BasicCacheContainer.DEFAULT_CACHE_NAME;
   private List<RemoteServerConfigurationBuilder> servers = new ArrayList<RemoteServerConfigurationBuilder>();
   private long socketTimeout = ConfigurationProperties.DEFAULT_SO_TIMEOUT;
   private boolean tcpNoDelay = true;
   private String transportFactory;
   private int valueSizeEstimate = ConfigurationProperties.DEFAULT_VALUE_SIZE;

   public RemoteStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
      asyncExecutorFactory = new ExecutorFactoryConfigurationBuilder(this);
      connectionPool = new ConnectionPoolConfigurationBuilder(this);
   }

   @Override
   public RemoteStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncExecutorFactory() {
      return asyncExecutorFactory;
   }

   @Override
   public RemoteStoreConfigurationBuilder balancingStrategy(String balancingStrategy) {
      this.balancingStrategy = balancingStrategy;
      return this;
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return connectionPool;
   }

   @Override
   public RemoteStoreConfigurationBuilder connectionTimeout(long connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      this.forceReturnValues = forceReturnValues;
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder hotRodWrapping(boolean hotRodWrapping) {
      this.hotRodWrapping = hotRodWrapping;
      this.rawValues(true);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder keySizeEstimate(int keySizeEstimate) {
      this.keySizeEstimate = keySizeEstimate;
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder marshaller(String marshaller) {
      this.marshaller = marshaller;
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller) {
      this.marshaller = marshaller.getName();
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder pingOnStartup(boolean pingOnStartup) {
      this.pingOnStartup = pingOnStartup;
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder protocolVersion(String protocolVersion) {
      this.protocolVersion = protocolVersion;
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder rawValues(boolean rawValues) {
      this.rawValues = rawValues;
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder remoteCacheName(String remoteCacheName) {
      this.remoteCacheName = remoteCacheName;
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder socketTimeout(long socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      this.tcpNoDelay = tcpNoDelay;
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder transportFactory(String transportFactory) {
      this.transportFactory = transportFactory;
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder transportFactory(Class<? extends TransportFactory> transportFactory) {
      this.transportFactory = transportFactory.getName();
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder valueSizeEstimate(int valueSizeEstimate) {
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
   public RemoteStoreConfiguration create() {
      List<RemoteServerConfiguration> remoteServers = new ArrayList<RemoteServerConfiguration>();
      for (RemoteServerConfigurationBuilder server : servers) {
         remoteServers.add(server.create());
      }
      return new RemoteStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                               singletonStore.create(), preload, shared, properties, asyncExecutorFactory.create(),
                                               balancingStrategy, connectionPool.create(), connectionTimeout,
                                               forceReturnValues, hotRodWrapping, keySizeEstimate,
                                               marshaller, pingOnStartup, protocolVersion, rawValues, remoteCacheName,
                                               remoteServers, socketTimeout, tcpNoDelay, transportFactory,
                                               valueSizeEstimate);
   }

   @Override
   public RemoteStoreConfigurationBuilder read(RemoteStoreConfiguration template) {
      this.asyncExecutorFactory.read(template.asyncExecutorFactory());
      this.balancingStrategy = template.balancingStrategy();
      this.connectionPool.read(template.connectionPool());
      this.connectionTimeout = template.connectionTimeout();
      this.forceReturnValues = template.forceReturnValues();
      this.hotRodWrapping = template.hotRodWrapping();
      this.keySizeEstimate = template.keySizeEstimate();
      this.marshaller = template.marshaller();
      this.pingOnStartup = template.pingOnStartup();
      this.protocolVersion = template.protocolVersion();
      this.rawValues = template.rawValues();
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
      async.read(template.async());
      singletonStore.read(template.singletonStore());
      return this;
   }

   @Override
   public void validate() {
      this.connectionPool.validate();
      this.asyncExecutorFactory.validate();
      for(RemoteServerConfigurationBuilder server : servers) {
         server.validate();
      }

      if (hotRodWrapping && marshaller != null) {
            throw log.cannotEnableHotRodWrapping();
      }
   }
}

package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.BALANCING_STRATEGY;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.CONNECTION_TIMEOUT;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.FORCE_RETURN_VALUES;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.HOTROD_WRAPPING;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.KEY_SIZE_ESTIMATE;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.MARSHALLER;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.PING_ON_STARTUP;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.PROTOCOL_VERSION;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.RAW_VALUES;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.REMOTE_CACHE_NAME;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.SERVERS;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.SOCKET_TIMEOUT;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.TCP_NO_DELAY;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.TRANSPORT_FACTORY;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.VALUE_SIZE_ESTIMATE;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * RemoteStoreConfigurationBuilder. Configures a
 * {@link org.infinispan.persistence.remote.RemoteStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class RemoteStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RemoteStoreConfiguration, RemoteStoreConfigurationBuilder> implements
      RemoteStoreConfigurationChildBuilder<RemoteStoreConfigurationBuilder> {
   private static final Log log = LogFactory.getLog(RemoteStoreConfigurationBuilder.class, Log.class);
   private final ExecutorFactoryConfigurationBuilder asyncExecutorFactory;
   private final ConnectionPoolConfigurationBuilder connectionPool;
   private List<RemoteServerConfigurationBuilder> servers = new ArrayList<RemoteServerConfigurationBuilder>();

   public RemoteStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, RemoteStoreConfiguration.attributeDefinitionSet());
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
      attributes.attribute(BALANCING_STRATEGY).set(balancingStrategy);
      return this;
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return connectionPool;
   }

   @Override
   public RemoteStoreConfigurationBuilder connectionTimeout(long connectionTimeout) {
      attributes.attribute(CONNECTION_TIMEOUT).set(connectionTimeout);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      attributes.attribute(FORCE_RETURN_VALUES).set(forceReturnValues);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder hotRodWrapping(boolean hotRodWrapping) {
      attributes.attribute(HOTROD_WRAPPING).set(hotRodWrapping);
      this.rawValues(true);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder keySizeEstimate(int keySizeEstimate) {
      attributes.attribute(KEY_SIZE_ESTIMATE).set(keySizeEstimate);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder marshaller(String marshaller) {
      attributes.attribute(MARSHALLER).set(marshaller);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller) {
      marshaller(marshaller.getName());
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder pingOnStartup(boolean pingOnStartup) {
      attributes.attribute(PING_ON_STARTUP).set(pingOnStartup);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder protocolVersion(String protocolVersion) {
      attributes.attribute(PROTOCOL_VERSION).set(protocolVersion);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder rawValues(boolean rawValues) {
      attributes.attribute(RAW_VALUES).set(rawValues);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder remoteCacheName(String remoteCacheName) {
      attributes.attribute(REMOTE_CACHE_NAME).set(remoteCacheName);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder socketTimeout(long socketTimeout) {
      attributes.attribute(SOCKET_TIMEOUT).set(socketTimeout);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      attributes.attribute(TCP_NO_DELAY).set(tcpNoDelay);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder transportFactory(String transportFactory) {
      attributes.attribute(TRANSPORT_FACTORY).set(transportFactory);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder transportFactory(Class<? extends TransportFactory> transportFactory) {
      transportFactory(transportFactory.getName());
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder valueSizeEstimate(int valueSizeEstimate) {
      attributes.attribute(VALUE_SIZE_ESTIMATE).set(valueSizeEstimate);
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
      attributes.attribute(SERVERS).set(remoteServers);
      return new RemoteStoreConfiguration(attributes.protect(), async.create(), singletonStore.create(), asyncExecutorFactory.create(), connectionPool.create());
   }

   @Override
   public RemoteStoreConfigurationBuilder read(RemoteStoreConfiguration template) {
      super.read(template);
      this.asyncExecutorFactory.read(template.asyncExecutorFactory());
      this.connectionPool.read(template.connectionPool());
      for (RemoteServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }

      return this;
   }

   @Override
   public void validate() {
      this.connectionPool.validate();
      this.asyncExecutorFactory.validate();
      for (RemoteServerConfigurationBuilder server : servers) {
         server.validate();
      }

      if (attributes.attribute(HOTROD_WRAPPING).get() && attributes.attribute(MARSHALLER).get() != null) {
         throw log.cannotEnableHotRodWrapping();
      }
   }
}

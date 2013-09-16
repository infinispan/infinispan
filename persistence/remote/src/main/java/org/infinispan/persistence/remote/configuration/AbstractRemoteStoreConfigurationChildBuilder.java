package org.infinispan.persistence.remote.configuration;

import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.AbstractStoreConfigurationChildBuilder;

/**
 * AbstractRemoteStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractRemoteStoreConfigurationChildBuilder<S> extends AbstractStoreConfigurationChildBuilder<S> implements RemoteStoreConfigurationChildBuilder<S> {
   private final RemoteStoreConfigurationBuilder builder;

   protected AbstractRemoteStoreConfigurationChildBuilder(RemoteStoreConfigurationBuilder builder) {
      super(builder);
      this.builder = builder;
   }

   @Override
   public RemoteServerConfigurationBuilder addServer() {
      return builder.addServer();
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncExecutorFactory() {
      return builder.asyncExecutorFactory();
   }

   @Override
   public RemoteStoreConfigurationBuilder balancingStrategy(String balancingStrategy) {
      return builder.balancingStrategy(balancingStrategy);
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return builder.connectionPool();
   }

   @Override
   public RemoteStoreConfigurationBuilder connectionTimeout(long connectionTimeout) {
      return builder.connectionTimeout(connectionTimeout);
   }

   @Override
   public RemoteStoreConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      return builder.forceReturnValues(forceReturnValues);
   }

   @Override
   public RemoteStoreConfigurationBuilder hotRodWrapping(boolean hotRodWrapping) {
      return builder.hotRodWrapping(hotRodWrapping);
   }

   @Override
   public RemoteStoreConfigurationBuilder keySizeEstimate(int keySizeEstimate) {
      return builder.keySizeEstimate(keySizeEstimate);
   }

   @Override
   public RemoteStoreConfigurationBuilder marshaller(String marshaller) {
      return builder.marshaller(marshaller);
   }

   @Override
   public RemoteStoreConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller) {
      return builder.marshaller(marshaller);
   }

   @Override
   public RemoteStoreConfigurationBuilder pingOnStartup(boolean pingOnStartup) {
      return builder.pingOnStartup(pingOnStartup);
   }

   @Override
   public RemoteStoreConfigurationBuilder protocolVersion(String protocolVersion) {
      return builder.protocolVersion(protocolVersion);
   }

   @Override
   public RemoteStoreConfigurationBuilder rawValues(boolean rawValues) {
      return builder.rawValues(rawValues);
   }

   @Override
   public RemoteStoreConfigurationBuilder remoteCacheName(String remoteCacheName) {
      return builder.remoteCacheName(remoteCacheName);
   }

   @Override
   public RemoteStoreConfigurationBuilder socketTimeout(long socketTimeout) {
      return builder.socketTimeout(socketTimeout);
   }

   @Override
   public RemoteStoreConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      return builder.tcpNoDelay(tcpNoDelay);
   }

   @Override
   public RemoteStoreConfigurationBuilder transportFactory(String transportFactory) {
      return builder.transportFactory(transportFactory);
   }

   @Override
   public RemoteStoreConfigurationBuilder transportFactory(Class<? extends TransportFactory> transportFactory) {
      return builder.transportFactory(transportFactory);
   }

   @Override
   public RemoteStoreConfigurationBuilder valueSizeEstimate(int valueSizeEstimate) {
      return builder.valueSizeEstimate(valueSizeEstimate);
   }
}

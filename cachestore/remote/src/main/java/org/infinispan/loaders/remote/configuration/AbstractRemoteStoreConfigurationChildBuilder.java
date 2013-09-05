package org.infinispan.loaders.remote.configuration;

import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.AbstractStoreConfigurationChildBuilder;
import org.infinispan.loaders.remote.wrapper.EntryWrapper;

/**
 * AbstractRemoteStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractRemoteStoreConfigurationChildBuilder<S> extends AbstractStoreConfigurationChildBuilder<S> implements RemoteCacheStoreConfigurationChildBuilder<S> {
   private final RemoteCacheStoreConfigurationBuilder builder;

   protected AbstractRemoteStoreConfigurationChildBuilder(RemoteCacheStoreConfigurationBuilder builder) {
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
   public RemoteCacheStoreConfigurationBuilder balancingStrategy(String balancingStrategy) {
      return builder.balancingStrategy(balancingStrategy);
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return builder.connectionPool();
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder connectionTimeout(long connectionTimeout) {
      return builder.connectionTimeout(connectionTimeout);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder entryWrapper(EntryWrapper<?, ?> entryWrapper) {
      return builder.entryWrapper(entryWrapper);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      return builder.forceReturnValues(forceReturnValues);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder hotRodWrapping(boolean hotRodWrapping) {
      return builder.hotRodWrapping(hotRodWrapping);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder keySizeEstimate(int keySizeEstimate) {
      return builder.keySizeEstimate(keySizeEstimate);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder marshaller(String marshaller) {
      return builder.marshaller(marshaller);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller) {
      return builder.marshaller(marshaller);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder pingOnStartup(boolean pingOnStartup) {
      return builder.pingOnStartup(pingOnStartup);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder protocolVersion(String protocolVersion) {
      return builder.protocolVersion(protocolVersion);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder rawValues(boolean rawValues) {
      return builder.rawValues(rawValues);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder remoteCacheName(String remoteCacheName) {
      return builder.remoteCacheName(remoteCacheName);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder socketTimeout(long socketTimeout) {
      return builder.socketTimeout(socketTimeout);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      return builder.tcpNoDelay(tcpNoDelay);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder transportFactory(String transportFactory) {
      return builder.transportFactory(transportFactory);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder transportFactory(Class<? extends TransportFactory> transportFactory) {
      return builder.transportFactory(transportFactory);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder valueSizeEstimate(int valueSizeEstimate) {
      return builder.valueSizeEstimate(valueSizeEstimate);
   }
}

package org.infinispan.persistence.remote.configuration;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.AbstractStoreConfigurationChildBuilder;

/**
 * AbstractRemoteStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractRemoteStoreConfigurationChildBuilder<S> extends AbstractStoreConfigurationChildBuilder<S> implements RemoteStoreConfigurationChildBuilder<S> {
   protected final RemoteStoreConfigurationBuilder builder;
   protected final AttributeSet attributes;

   protected AbstractRemoteStoreConfigurationChildBuilder(RemoteStoreConfigurationBuilder builder, AttributeSet attributes) {
      super(builder);
      this.attributes = attributes;
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
   public RemoteStoreConfigurationBuilder marshaller(String marshaller) {
      return builder.marshaller(marshaller);
   }

   @Override
   public RemoteStoreConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller) {
      return builder.marshaller(marshaller);
   }

   @Override
   public RemoteStoreConfigurationBuilder protocolVersion(ProtocolVersion protocolVersion) {
      return builder.protocolVersion(protocolVersion);
   }

   @Override
   public RemoteStoreConfigurationBuilder remoteCacheContainer(String name) {
      return builder.remoteCacheContainer(name);
   }

   @Override
   public RemoteStoreConfigurationBuilder remoteCacheName(String remoteCacheName) {
      return builder.remoteCacheName(remoteCacheName);
   }

   @Override
   public SecurityConfigurationBuilder remoteSecurity() {
      return builder.remoteSecurity();
   }

   @Override
   public RemoteStoreConfigurationBuilder socketTimeout(long socketTimeout) {
      return builder.socketTimeout(socketTimeout);
   }

   @Override
   public RemoteStoreConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      return builder.tcpNoDelay(tcpNoDelay);
   }

   public RemoteStoreConfigurationBuilder getRemoteStoreBuilder() {
      return builder;
   }
}

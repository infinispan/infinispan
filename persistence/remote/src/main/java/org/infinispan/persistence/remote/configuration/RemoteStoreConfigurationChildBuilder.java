package org.infinispan.persistence.remote.configuration;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.StoreConfigurationChildBuilder;
import org.infinispan.container.entries.InternalCacheEntry;

public interface RemoteStoreConfigurationChildBuilder<S> extends StoreConfigurationChildBuilder<S> {

   /**
    * Adds a new remote server
    */
   RemoteServerConfigurationBuilder addServer();

   /**
    * Configuration for the executor service used for asynchronous work on the Transport, including
    * asynchronous marshalling and Cache 'async operations' such as Cache.putAsync().
    */
   ExecutorFactoryConfigurationBuilder asyncExecutorFactory();

   /**
    * For replicated (vs distributed) Hot Rod server clusters, the client balances requests to the
    * servers according to this strategy.
    */
   RemoteStoreConfigurationBuilder balancingStrategy(String balancingStrategy);

   /**
    * Configures the connection pool
    */
   ConnectionPoolConfigurationBuilder connectionPool();

   /**
    * This property defines the maximum socket connect timeout before giving up connecting to the
    * server.
    */
   RemoteStoreConfigurationBuilder connectionTimeout(long connectionTimeout);

   /**
    * Whether or not to implicitly FORCE_RETURN_VALUE for all calls.
    */
   RemoteStoreConfigurationBuilder forceReturnValues(boolean forceReturnValues);

   /**
    * Configures this RemoteStore so that it enables all settings needed to create entries to be served
    * by a HotRod endpoint, for example when performing rolling upgrades.
    */
   RemoteStoreConfigurationBuilder hotRodWrapping(boolean hotRodWrapping);

   /**
    * The class name of the driver used for connecting to the database.
    */
   RemoteStoreConfigurationBuilder keySizeEstimate(int keySizeEstimate);

   /**
    * Allows you to specify a custom {@link org.infinispan.commons.marshall.Marshaller} implementation to
    * serialize and deserialize user objects.
    */
   RemoteStoreConfigurationBuilder marshaller(String marshaller);

   /**
    * Allows you to specify a custom {@link org.infinispan.commons.marshall.Marshaller} implementation to
    * serialize and deserialize user objects.
    */
   RemoteStoreConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller);

   /**
    * This property defines the protocol version that this client should use. Defaults to {@link ProtocolVersion#DEFAULT_PROTOCOL_VERSION}
    * @deprecated Use {@link #protocolVersion(ProtocolVersion)} instead
    */
   @Deprecated
   RemoteStoreConfigurationBuilder protocolVersion(String protocolVersion);

   /**
    * This property defines the protocol version that this client should use. Defaults to {@link ProtocolVersion#DEFAULT_PROTOCOL_VERSION}
    */
   RemoteStoreConfigurationBuilder protocolVersion(ProtocolVersion protocolVersion);

   /**
    * Normally the {@link org.infinispan.persistence.remote.RemoteStore} stores values wrapped in {@link InternalCacheEntry}. Setting
    * this property to true causes the raw values to be stored instead for interoperability with direct
    * access by {@link RemoteCacheManager}s
    */
   RemoteStoreConfigurationBuilder rawValues(boolean rawValues);

   /**
    * The name of the remote cache in the remote infinispan cluster, to which to connect to. If
    * unspecified, the default cache will be used
    */
   RemoteStoreConfigurationBuilder remoteCacheName(String remoteCacheName);


   /**
    * Configures connection security
    */
   SecurityConfigurationBuilder remoteSecurity();

   /**
    * This property defines the maximum socket read timeout in milliseconds before giving up waiting
    * for bytes from the server. Defaults to 60000 (1 minute)
    */
   RemoteStoreConfigurationBuilder socketTimeout(long socketTimeout);

   /**
    * Affects TCP NODELAY on the TCP stack. Defaults to enabled
    */
   RemoteStoreConfigurationBuilder tcpNoDelay(boolean tcpNoDelay);

   /**
    * Controls which transport to use. Currently only the TcpTransport is supported.
    */
   RemoteStoreConfigurationBuilder transportFactory(String transportFactory);

   /**
    * Controls which transport to use. Currently only the TcpTransport is supported.
    */
   RemoteStoreConfigurationBuilder transportFactory(Class<? extends ChannelFactory> transportFactory);

   /**
    * This hint allows sizing of byte buffers when serializing and deserializing values, to minimize
    * array resizing.
    */
   RemoteStoreConfigurationBuilder valueSizeEstimate(int valueSizeEstimate);

}

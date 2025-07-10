package org.infinispan.persistence.remote.configuration;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.StoreConfigurationChildBuilder;

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
   @Deprecated(forRemoval = true, since = "15.1")
   ConnectionPoolConfigurationBuilder connectionPool();

   /**
    * This property defines the maximum socket connect timeout before giving up connecting to the
    * server.
    */
   RemoteStoreConfigurationBuilder connectionTimeout(long connectionTimeout);

   /**
    * Whether to implicitly FORCE_RETURN_VALUE for all calls.
    */
   RemoteStoreConfigurationBuilder forceReturnValues(boolean forceReturnValues);

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
    */
   RemoteStoreConfigurationBuilder protocolVersion(ProtocolVersion protocolVersion);

   /**
    * Specifies the name of a shared remote cache container to use, instead of creating a dedicated instance.
    */
   RemoteStoreConfigurationBuilder remoteCacheContainer(String name);

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

}

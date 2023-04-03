package org.infinispan.hotrod.configuration;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.hotrod.impl.consistenthash.ConsistentHashV2;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * ConfigurationChildBuilder.
 *
 * @since 14.0
 */
public interface ConfigurationChildBuilder {

   /**
    * Adds a new remote server
    */
   ServerConfigurationBuilder addServer();

   /**
    * Adds a new remote server cluster
    */
   ClusterConfigurationBuilder addCluster(String clusterName);

   /**
    * Adds a list of remote servers in the form: host1[:port][;host2[:port]]...
    */
   HotRodConfigurationBuilder addServers(String servers);

   /**
    * Configuration for the executor service used for asynchronous work on the Transport, including
    * asynchronous marshalling and Cache 'async operations' such as Cache.putAsync().
    */
   ExecutorFactoryConfigurationBuilder asyncExecutorFactory();

   /**
    * For replicated (vs distributed) Hot Rod server clusters, the client balances requests to the
    * servers according to this strategy.
    */
   HotRodConfigurationBuilder balancingStrategy(String balancingStrategy);

   /**
    * For replicated (vs distributed) Hot Rod server clusters, the client balances requests to the
    * servers according to this strategy.
    */
   HotRodConfigurationBuilder balancingStrategy(Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory);

   /**
    * For replicated (vs distributed) Hot Rod server clusters, the client balances requests to the
    * servers according to this strategy.
    */
   HotRodConfigurationBuilder balancingStrategy(Class<? extends FailoverRequestBalancingStrategy> balancingStrategy);

   /**
    * Specifies the level of "intelligence" the client should have
    */
   HotRodConfigurationBuilder clientIntelligence(ClientIntelligence clientIntelligence);

   /**
    * Configures the connection pool
    */
   ConnectionPoolConfigurationBuilder connectionPool();

   /**
    * This property defines the maximum socket connect timeout in milliseconds before giving up connecting to the
    * server. Defaults to 2000 (2 seconds).
    */
   HotRodConfigurationBuilder connectionTimeout(int connectionTimeout);

   /**
    * Defines the {@link ConsistentHash} implementation to use for the specified version. By default,
    * {@link ConsistentHashV2} is used for version 1 and {@link ConsistentHashV2} is used for version 2.
    */
   HotRodConfigurationBuilder consistentHashImpl(int version, Class<? extends ConsistentHash> consistentHashClass);

   /**
    * Defines the {@link ConsistentHash} implementation to use for the specified version. By default,
    * {@link ConsistentHashV2} is used for version 1 and {@link ConsistentHashV2} is used for version 2.
    */
   HotRodConfigurationBuilder consistentHashImpl(int version, String consistentHashClass);

   HotRodConfigurationBuilder dnsResolverMinTTL(int ttl);

   HotRodConfigurationBuilder dnsResolverMaxTTL(int ttl);

   HotRodConfigurationBuilder dnsResolverNegativeTTL(int ttl);

   /**
    * Whether or not to implicitly FORCE_RETURN_VALUE for all calls.
    */
   HotRodConfigurationBuilder forceReturnValues(boolean forceReturnValues);

   /**
    * Allows you to specify a custom {@link Marshaller} implementation to
    * serialize and deserialize user objects. This method is mutually exclusive with {@link #marshaller(Marshaller)}.
    */
   HotRodConfigurationBuilder marshaller(String marshaller);

   /**
    * Allows you to specify a custom {@link Marshaller} implementation to
    * serialize and deserialize user objects. This method is mutually exclusive with {@link #marshaller(Marshaller)}.
    */
   HotRodConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller);

   /**
    * Allows you to specify an instance of {@link Marshaller} to serialize
    * and deserialize user objects. This method is mutually exclusive with {@link #marshaller(Class)}.
    */
   HotRodConfigurationBuilder marshaller(Marshaller marshaller);

   /**
    * Supply a {@link SerializationContextInitializer} implementation to register classes with the {@link
    * org.infinispan.commons.marshall.ProtoStreamMarshaller}'s {@link org.infinispan.protostream.SerializationContext}.
    */
   HotRodConfigurationBuilder addContextInitializer(String contextInitializer);

   /**
    * Supply a {@link SerializationContextInitializer} implementation to register classes with the {@link
    * org.infinispan.commons.marshall.ProtoStreamMarshaller}'s {@link org.infinispan.protostream.SerializationContext}.
    */
   HotRodConfigurationBuilder addContextInitializer(SerializationContextInitializer contextInitializer);

   /**
    * Convenience method to supply multiple {@link SerializationContextInitializer} implementations.
    *
    * @see #addContextInitializer(SerializationContextInitializer).
    */
   HotRodConfigurationBuilder addContextInitializers(SerializationContextInitializer... contextInitializers);

   /**
    * This property defines the protocol version that this client should use. Defaults to the latest protocol version
    * supported by this client.
    */
   HotRodConfigurationBuilder version(ProtocolVersion protocolVersion);

   /**
    * This property defines the maximum socket read timeout in milliseconds before giving up waiting
    * for bytes from the server. Defaults to 2000 (2 seconds)
    */
   HotRodConfigurationBuilder socketTimeout(int socketTimeout);

   /**
    * Security Configuration
    */
   SecurityConfigurationBuilder security();

   /**
    * Affects TCP NODELAY on the TCP stack. Defaults to enabled
    */
   HotRodConfigurationBuilder tcpNoDelay(boolean tcpNoDelay);

   /**
    * Affects TCP KEEPALIVE on the TCP stack. Defaults to disable
    */
   HotRodConfigurationBuilder tcpKeepAlive(boolean keepAlive);

   /**
    * Configures this builder using the specified URI.
    */
   HotRodConfigurationBuilder uri(URI uri);

   /**
    * Configures this builder using the specified URI.
    */
   HotRodConfigurationBuilder uri(String uri);

   /**
    * It sets the maximum number of retries for each request. A valid value should be greater or equals than 0 (zero).
    * Zero means no retry will made in case of a network failure. It defaults to 3.
    */
   HotRodConfigurationBuilder maxRetries(int maxRetries);

   /**
    * List of regular expressions for classes that can be deserialized using standard Java deserialization
    * when reading data that might have been stored with a different endpoint, e.g. REST.
    */
   HotRodConfigurationBuilder addJavaSerialAllowList(String... regEx);

   /**
    * Sets the batch size of internal iterators (ie. <code>keySet().iterator()</code>. Defaults to 10_000
    * @param batchSize the batch size to set
    * @return this configuration builder with the batch size set
    */
   HotRodConfigurationBuilder batchSize(int batchSize);

   /**
    * Configures client-side statistics.
    */
   StatisticsConfigurationBuilder statistics();

   /**
    * Per-cache configuration
    * @param name the name of the cache to which specific configuration should be applied. You may use wildcard globbing (e.g. <code>cache-*</code>) which will apply to any cache that matches.
    * @return the {@link RemoteCacheConfigurationBuilder} for the cache
    */
   RemoteCacheConfigurationBuilder remoteCache(String name);

   /**
    * Sets the transaction's timeout.
    * <p>
    * This timeout is used by the server to rollback unrecoverable transaction when they are idle for this amount of
    * time.
    * <p>
    * An unrecoverable transaction are transaction enlisted as Synchronization ({@link TransactionMode#NON_XA})
    * or XAResource without recovery enabled ({@link TransactionMode#NON_DURABLE_XA}).
    * <p>
    * For XAResource, this value is overwritten by XAResource#setTransactionTimeout(int).
    * <p>
    * It defaults to 1 minute.
    */
   HotRodConfigurationBuilder transactionTimeout(long timeout, TimeUnit timeUnit);

   /**
    * Set the TransportFactory. It defaults to {@link org.infinispan.hotrod.impl.transport.netty.DefaultTransportFactory}
    * @param transportFactory an instance of {@link TransportFactory}
    */
   HotRodConfigurationBuilder transportFactory(TransportFactory transportFactory);

   /**
    * Configures this builder using the specified properties. See {@link HotRodConfigurationBuilder} for a list.
    */
   HotRodConfigurationBuilder withProperties(Properties properties);

   /**
    * Builds a configuration object
    */
   HotRodConfiguration build();

}

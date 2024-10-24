package org.infinispan.client.hotrod.configuration;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.transaction.xa.XAResource;

import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.TransportFactory;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV2;
import org.infinispan.client.hotrod.metrics.RemoteCacheManagerMetricsRegistry;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.SerializationContextInitializer;

import jakarta.transaction.Synchronization;

/**
 * ConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
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
   ConfigurationBuilder addServers(String servers);

   /**
    * Configuration for the executor service used for asynchronous work on the Transport, including
    * asynchronous marshalling and Cache 'async operations' such as Cache.putAsync().
    */
   ExecutorFactoryConfigurationBuilder asyncExecutorFactory();

   /**
    * For replicated (vs distributed) Hot Rod server clusters, the client balances requests to the
    * servers according to this strategy.
    */
   ConfigurationBuilder balancingStrategy(String balancingStrategy);

   /**
    * For replicated (vs distributed) Hot Rod server clusters, the client balances requests to the
    * servers according to this strategy.
    */
   ConfigurationBuilder balancingStrategy(Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory);

   /**
    * For replicated (vs distributed) Hot Rod server clusters, the client balances requests to the
    * servers according to this strategy.
    */
   ConfigurationBuilder balancingStrategy(Class<? extends FailoverRequestBalancingStrategy> balancingStrategy);

   /**
    * Specifies the level of "intelligence" the client should have
    */
   ConfigurationBuilder clientIntelligence(ClientIntelligence clientIntelligence);

   /**
    * Configures the connection pool
    */
   ConnectionPoolConfigurationBuilder connectionPool();

   /**
    * This property defines the maximum socket connect timeout in milliseconds before giving up connecting to the
    * server. Defaults to {@link org.infinispan.client.hotrod.impl.ConfigurationProperties#DEFAULT_CONNECT_TIMEOUT}
    */
   ConfigurationBuilder connectionTimeout(int connectionTimeout);

   /**
    * Defines the {@link ConsistentHash} implementation to use for the specified version. By default,
    * {@link ConsistentHashV2} is used for version 1 and {@link ConsistentHashV2} is used for version 2.
    */
   ConfigurationBuilder consistentHashImpl(int version, Class<? extends ConsistentHash> consistentHashClass);

   /**
    * Defines the {@link ConsistentHash} implementation to use for the specified version. By default,
    * {@link ConsistentHashV2} is used for version 1 and {@link ConsistentHashV2} is used for version 2.
    */
   ConfigurationBuilder consistentHashImpl(int version, String consistentHashClass);

   ConfigurationBuilder dnsResolverMinTTL(int minTTL);

   ConfigurationBuilder dnsResolverMaxTTL(int maxTTL);

   ConfigurationBuilder dnsResolverNegativeTTL(int negativeTTL);

   /**
    * Whether or not to implicitly FORCE_RETURN_VALUE for all calls.
    */
   ConfigurationBuilder forceReturnValues(boolean forceReturnValues);

   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   ConfigurationBuilder keySizeEstimate(int keySizeEstimate);

   /**
    * Allows you to specify a custom {@link Marshaller} implementation to
    * serialize and deserialize user objects. This method is mutually exclusive with {@link #marshaller(Marshaller)}.
    */
   ConfigurationBuilder marshaller(String marshaller);

   /**
    * Allows you to specify a custom {@link Marshaller} implementation to
    * serialize and deserialize user objects. This method is mutually exclusive with {@link #marshaller(Marshaller)}.
    */
   ConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller);

   /**
    * Allows you to specify an instance of {@link Marshaller} to serialize
    * and deserialize user objects. This method is mutually exclusive with {@link #marshaller(Class)}.
    */
   ConfigurationBuilder marshaller(Marshaller marshaller);

   /**
    * Supply a {@link SerializationContextInitializer} implementation to register classes with the {@link
    * org.infinispan.commons.marshall.ProtoStreamMarshaller}'s {@link org.infinispan.protostream.SerializationContext}.
    */
   ConfigurationBuilder addContextInitializer(String contextInitializer);

   /**
    * Supply a {@link SerializationContextInitializer} implementation to register classes with the {@link
    * org.infinispan.commons.marshall.ProtoStreamMarshaller}'s {@link org.infinispan.protostream.SerializationContext}.
    */
   ConfigurationBuilder addContextInitializer(SerializationContextInitializer contextInitializer);

   /**
    * Convenience method to supply multiple {@link SerializationContextInitializer} implementations.
    *
    * @see #addContextInitializer(SerializationContextInitializer).
    */
   ConfigurationBuilder addContextInitializers(SerializationContextInitializer... contextInitializers);

   /**
    * This property defines the protocol version that this client should use. Defaults to the latest protocol version
    * supported by this client.
    */
   ConfigurationBuilder version(ProtocolVersion protocolVersion);

   /**
    * This property defines the maximum socket read timeout in milliseconds before giving up waiting
    * for bytes from the server. Defaults to {@link org.infinispan.client.hotrod.impl.ConfigurationProperties#DEFAULT_SO_TIMEOUT}
    */
   ConfigurationBuilder socketTimeout(int socketTimeout);

   /**
    * Security Configuration
    */
   SecurityConfigurationBuilder security();

   /**
    * Affects TCP NODELAY on the TCP stack. Defaults to enabled
    */
   ConfigurationBuilder tcpNoDelay(boolean tcpNoDelay);

   /**
    * Affects TCP KEEPALIVE on the TCP stack. Defaults to disable
    */
   ConfigurationBuilder tcpKeepAlive(boolean keepAlive);

   /**
    * Configures this builder using the specified URI.
    */
   ConfigurationBuilder uri(URI uri);

   /**
    * Configures this builder using the specified URI.
    */
   ConfigurationBuilder uri(String uri);

   /**
    * @deprecated Since 12.0, does nothing and will be removed in 15.0
    */
   @Deprecated(forRemoval=true, since = "12.0")
   ConfigurationBuilder valueSizeEstimate(int valueSizeEstimate);

   /**
    * It sets the maximum number of retries for each request. A valid value should be greater or equals than 0 (zero).
    * Zero means no retry will made in case of a network failure. It defaults to 10.
    */
   ConfigurationBuilder maxRetries(int maxRetries);

   /**
    * The time for a failed server to be cleared allowing for it to attempt to reconnect at a later point.
    * <p>
    * If the value is less than or equal to 0 it will be disabled, meaning a failed SERVER will not be reconnected to
    * until all configured servers have failed or a topology update (TOPOLOGY_AWARE and CONSISTENT_HASH_AWARE only)
    * @param timeoutInMilliseconds the timeout to attempt to clear a failed server in milliseconds
    * @return this bulider
    */
   ConfigurationBuilder serverFailureTimeout(int timeoutInMilliseconds);

   /**
    * List of regular expressions for classes that can be deserialized using standard Java deserialization
    * when reading data that might have been stored with a different endpoint, e.g. REST.
    */
   ConfigurationBuilder addJavaSerialAllowList(String... regEx);

   /**
    * @deprecated Use {@link #addJavaSerialAllowList(String...)} instead. To be removed in 14.0.
    */
   @Deprecated(forRemoval=true, since = "12.0")
   ConfigurationBuilder addJavaSerialWhiteList(String... regEx);

   /**
    * Sets the batch size of internal iterators (ie. <code>keySet().iterator()</code>. Defaults to 10_000
    * @param batchSize the batch size to set
    * @return this configuration builder with the batch size set
    */
   ConfigurationBuilder batchSize(int batchSize);

   /**
    * Configures client-side statistics.
    */
   StatisticsConfigurationBuilder statistics();

   /**
    * Transaction configuration
    */
   TransactionConfigurationBuilder transaction();

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
    * An unrecoverable transaction are transaction enlisted as {@link Synchronization} ({@link TransactionMode#NON_XA})
    * or {@link XAResource} without recovery enabled ({@link TransactionMode#NON_DURABLE_XA}).
    * <p>
    * For {@link XAResource}, this value is overwritten by {@link XAResource#setTransactionTimeout(int)}.
    * <p>
    * It defaults to 1 minute.
    */
   ConfigurationBuilder transactionTimeout(long timeout, TimeUnit timeUnit);

   /**
    * Set the TransportFactory. It defaults to {@link org.infinispan.client.hotrod.impl.transport.netty.DefaultTransportFactory}
    * @param transportFactory an instance of {@link TransportFactory}
    */
   ConfigurationBuilder transportFactory(TransportFactory transportFactory);

   /**
    * Configures this builder using the specified properties. See {@link ConfigurationBuilder} for a list.
    */
   ConfigurationBuilder withProperties(Properties properties);

   /**
    * Sets the {@link RemoteCacheManagerMetricsRegistry}.
    * <p>
    * The Hot Rod client will register metrics about connection pool and cache accesses using that instance.
    *
    * @param metricRegistry The {@link RemoteCacheManagerMetricsRegistry} implementation.
    */
   ConfigurationBuilder withMetricRegistry(RemoteCacheManagerMetricsRegistry metricRegistry);

   /**
    * Builds a configuration object
    */
   Configuration build();

}

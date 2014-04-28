package org.infinispan.client.hotrod.configuration;

import java.util.Properties;

import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV2;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.RequestBalancingStrategy;
import org.infinispan.commons.marshall.Marshaller;

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
   ConfigurationBuilder balancingStrategy(Class<? extends RequestBalancingStrategy> balancingStrategy);

   /**
    * @param classLoader
    * @return
    */
   ConfigurationBuilder classLoader(ClassLoader classLoader);

   /**
    * Configures the connection pool
    */
   ConnectionPoolConfigurationBuilder connectionPool();

   /**
    * This property defines the maximum socket connect timeout before giving up connecting to the
    * server.
    */
   ConfigurationBuilder connectionTimeout(int connectionTimeout);

   /**
    * Defines the {@link ConsistentHash} implementation to use for the specified version. By default,
    * {@link ConsistentHashV1} is used for version 1 and {@link ConsistentHashV2} is used for version 2.
    */
   ConfigurationBuilder consistentHashImpl(int version, Class<? extends ConsistentHash> consistentHashClass);

   /**
    * Defines the {@link ConsistentHash} implementation to use for the specified version. By default,
    * {@link ConsistentHashV1} is used for version 1 and {@link ConsistentHashV2} is used for version 2.
    */
   ConfigurationBuilder consistentHashImpl(int version, String consistentHashClass);

   /**
    * Whether or not to implicitly FORCE_RETURN_VALUE for all calls.
    */
   ConfigurationBuilder forceReturnValues(boolean forceReturnValues);

   /**
    * This hint allows sizing of byte buffers when serializing and deserializing keys, to minimize array resizing. It defaults to 64.
    */
   ConfigurationBuilder keySizeEstimate(int keySizeEstimate);

   /**
    * Allows you to specify a custom {@link org.infinispan.marshall.Marshaller} implementation to
    * serialize and deserialize user objects. This method is mutually exclusive with {@link #marshaller(Marshaller)}.
    */
   ConfigurationBuilder marshaller(String marshaller);

   /**
    * Allows you to specify a custom {@link org.infinispan.marshall.Marshaller} implementation to
    * serialize and deserialize user objects. This method is mutually exclusive with {@link #marshaller(Marshaller)}.
    */
   ConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller);

   /**
    * Allows you to specify an instance of {@link org.infinispan.marshall.Marshaller} to serialize
    * and deserialize user objects. This method is mutually exclusive with {@link #marshaller(Class)}.
    */
   ConfigurationBuilder marshaller(Marshaller marshaller);

   /**
    * If true, a ping request is sent to a back end server in order to fetch cluster's topology.
    */
   ConfigurationBuilder pingOnStartup(boolean pingOnStartup);

   /**
    * This property defines the protocol version that this client should use. Defaults to 1.1. Other
    * valid values include 1.0.
    */
   ConfigurationBuilder protocolVersion(String protocolVersion);

   /**
    * This property defines the maximum socket read timeout in milliseconds before giving up waiting
    * for bytes from the server. Defaults to 60000 (1 minute)
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
    * Controls which transport to use. Currently only the TcpTransport is supported.
    */
   ConfigurationBuilder transportFactory(String transportFactory);

   /**
    * Controls which transport to use. Currently only the TcpTransport is supported.
    */
   ConfigurationBuilder transportFactory(Class<? extends TransportFactory> transportFactory);

   /**
    * This hint allows sizing of byte buffers when serializing and deserializing values, to minimize
    * array resizing. It defaults to 512
    */
   ConfigurationBuilder valueSizeEstimate(int valueSizeEstimate);

   /**
    * It sets the maximum number of retries for each request. A valid value should be greater or equals than 0 (zero).
    * Zero means no retry will made in case of a network failure. It defaults to 10.
    */
   ConfigurationBuilder maxRetries(int maxRetries);

   /**
    * Configures this builder using the specified properties
    */
   ConfigurationBuilder withProperties(Properties properties);

   /**
    * Builds a configuration object
    */
   Configuration build();

}

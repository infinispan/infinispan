package org.infinispan.api.client.configuration.reactive;

import java.util.List;
import java.util.function.Supplier;

import org.infinispan.api.ClientConfig;
import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ClusterConfiguration;
import org.infinispan.client.hotrod.configuration.ConnectionPoolConfiguration;
import org.infinispan.client.hotrod.configuration.ExecutorFactoryConfiguration;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.configuration.SecurityConfiguration;
import org.infinispan.client.hotrod.configuration.ServerConfiguration;
import org.infinispan.client.hotrod.configuration.StatisticsConfiguration;
import org.infinispan.client.hotrod.configuration.TransactionConfiguration;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Features;

@BuiltBy(ConfigurationBuilder.class)
public class Configuration extends org.infinispan.client.hotrod.configuration.Configuration implements ClientConfig {

   public Configuration(ExecutorFactoryConfiguration asyncExecutorFactory, Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory, ClassLoader classLoader, ClientIntelligence clientIntelligence, ConnectionPoolConfiguration connectionPool, int connectionTimeout, Class<? extends ConsistentHash>[] consistentHashImpl, boolean forceReturnValues, int keySizeEstimate, Marshaller marshaller, Class<? extends Marshaller> marshallerClass, ProtocolVersion protocolVersion, List<ServerConfiguration> servers, int socketTimeout, SecurityConfiguration security, boolean tcpNoDelay, boolean tcpKeepAlive, int valueSizeEstimate, int maxRetries, NearCacheConfiguration nearCache, List<ClusterConfiguration> clusters, List<String> serialWhitelist, int batchSize, TransactionConfiguration transaction, StatisticsConfiguration statistics, Features features) {
      super(asyncExecutorFactory, balancingStrategyFactory, classLoader, clientIntelligence, connectionPool, connectionTimeout, consistentHashImpl, forceReturnValues, keySizeEstimate, marshaller, marshallerClass, protocolVersion, servers, socketTimeout, security, tcpNoDelay, tcpKeepAlive, valueSizeEstimate, maxRetries, nearCache, clusters, serialWhitelist, batchSize, transaction, statistics, features);
   }
}

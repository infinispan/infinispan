package org.infinispan.hotrod.configuration;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * AbstractConfigurationChildBuilder.
 *
 * @since 14.0
 */
public abstract class AbstractConfigurationChildBuilder implements ConfigurationChildBuilder {
   final HotRodConfigurationBuilder builder;

   protected AbstractConfigurationChildBuilder(HotRodConfigurationBuilder builder) {
      this.builder = builder;
   }

   @Override
   public ServerConfigurationBuilder addServer() {
      return builder.addServer();
   }

   @Override
   public ClusterConfigurationBuilder addCluster(String clusterName) {
      return builder.addCluster(clusterName);
   }

   @Override
   public HotRodConfigurationBuilder addServers(String servers) {
      return builder.addServers(servers);
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncExecutorFactory() {
      return builder.asyncExecutorFactory();
   }

   @Override
   public HotRodConfigurationBuilder balancingStrategy(String balancingStrategy) {
      return builder.balancingStrategy(balancingStrategy);
   }

   @Override
   public HotRodConfigurationBuilder balancingStrategy(Class<? extends FailoverRequestBalancingStrategy> balancingStrategy) {
      return builder.balancingStrategy(balancingStrategy);
   }

   @Override
   public HotRodConfigurationBuilder balancingStrategy(Supplier<FailoverRequestBalancingStrategy> balancingStrategyFactory) {
      return builder.balancingStrategy(balancingStrategyFactory);
   }

   @Override
   public HotRodConfigurationBuilder clientIntelligence(ClientIntelligence clientIntelligence) {
      return builder.clientIntelligence(clientIntelligence);
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return builder.connectionPool();
   }

   @Override
   public HotRodConfigurationBuilder connectionTimeout(int connectionTimeout) {
      return builder.connectionTimeout(connectionTimeout);
   }

   @Override
   public HotRodConfigurationBuilder consistentHashImpl(int version, Class<? extends ConsistentHash> consistentHashClass) {
      return builder.consistentHashImpl(version, consistentHashClass);
   }

   @Override
   public HotRodConfigurationBuilder consistentHashImpl(int version, String consistentHashClass) {
      return builder.consistentHashImpl(version, consistentHashClass);
   }

   @Override
   public HotRodConfigurationBuilder dnsResolverMinTTL(int ttl) {
      return builder.dnsResolverMinTTL(ttl);
   }

   @Override
   public HotRodConfigurationBuilder dnsResolverMaxTTL(int ttl) {
      return builder.dnsResolverMaxTTL(ttl);
   }

   @Override
   public HotRodConfigurationBuilder dnsResolverNegativeTTL(int ttl) {
      return builder.dnsResolverNegativeTTL(ttl);
   }

   @Override
   public HotRodConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      return builder.forceReturnValues(forceReturnValues);
   }

   @Override
   public HotRodConfigurationBuilder marshaller(String marshaller) {
      return builder.marshaller(marshaller);
   }

   @Override
   public HotRodConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller) {
      return builder.marshaller(marshaller);
   }

   @Override
   public HotRodConfigurationBuilder marshaller(Marshaller marshaller) {
      return builder.marshaller(marshaller);
   }

   @Override
   public HotRodConfigurationBuilder addContextInitializer(String contextInitializer) {
      return builder.addContextInitializer(contextInitializer);
   }

   @Override
   public HotRodConfigurationBuilder addContextInitializer(SerializationContextInitializer contextInitializer) {
      return builder.addContextInitializer(contextInitializer);
   }

   @Override
   public HotRodConfigurationBuilder addContextInitializers(SerializationContextInitializer... contextInitializers) {
      return builder.addContextInitializers(contextInitializers);
   }

   @Override
   public HotRodConfigurationBuilder version(ProtocolVersion protocolVersion) {
      return builder.version(protocolVersion);
   }

   @Override
   public HotRodConfigurationBuilder socketTimeout(int socketTimeout) {
      return builder.socketTimeout(socketTimeout);
   }

   @Override
   public SecurityConfigurationBuilder security() {
      return builder.security();
   }

   @Override
   public HotRodConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      return builder.tcpNoDelay(tcpNoDelay);
   }

   @Override
   public HotRodConfigurationBuilder tcpKeepAlive(boolean tcpKeepAlive) {
      return builder.tcpKeepAlive(tcpKeepAlive);
   }

   @Override
   public HotRodConfigurationBuilder maxRetries(int retriesPerServer) {
      return builder.maxRetries(retriesPerServer);
   }

   @Override
   public HotRodConfigurationBuilder addJavaSerialAllowList(String... regExs) {
      return builder.addJavaSerialAllowList(regExs);
   }

   @Override
   public HotRodConfigurationBuilder batchSize(int batchSize) {
      return builder.batchSize(batchSize);
   }

   @Override
   public StatisticsConfigurationBuilder statistics() {
      return builder.statistics();
   }

   @Override
   public RemoteCacheConfigurationBuilder remoteCache(String name) {
      return builder.remoteCache(name);
   }

   @Override
   public HotRodConfigurationBuilder transactionTimeout(long timeout, TimeUnit timeUnit) {
      return builder.transactionTimeout(timeout, timeUnit);
   }

   @Override
   public HotRodConfigurationBuilder transportFactory(TransportFactory transportFactory) {
      return builder.transportFactory(transportFactory);
   }

   @Override
   public HotRodConfigurationBuilder uri(URI uri) {
      return builder.uri(uri);
   }

   @Override
   public HotRodConfigurationBuilder uri(String uri) {
      return builder.uri(uri);
   }

   @Override
   public HotRodConfigurationBuilder withProperties(Properties properties) {
      return builder.withProperties(properties);
   }

   @Override
   public HotRodConfiguration build() {
      return builder.build();
   }

}

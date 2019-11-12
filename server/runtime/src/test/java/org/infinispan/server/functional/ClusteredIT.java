package org.infinispan.server.functional;

import static org.junit.Assert.assertFalse;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Index;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.infinispan.test.Exceptions;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({
      HotRodCacheOperations.class,
      RestOperations.class,
      RestRouter.class,
      RestServerResource.class,
      MemcachedOperations.class,
      HotRodCounterOperations.class,
      HotRodMultiMapOperations.class,
      HotRodTransactionalCacheOperations.class,
      HotRodCacheQueries.class,
      HotRodCacheContinuousQueries.class,
      HotRodListenerWithDslFilter.class,
      IgnoreCaches.class,
      RestMetricsResource.class
})
public class ClusteredIT {

   @ClassRule
   public static final InfinispanServerRule SERVERS = new InfinispanServerRule(
         new InfinispanServerRuleConfigurationBuilder("configuration/ClusteredServerTest.xml")
               .numServers(2)
   );

   static <K, V> RemoteCache<K, V> createQueryableCache(InfinispanServerTestMethodRule testMethodRule, boolean indexed) {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.marshaller(new ProtoStreamMarshaller());

      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).stateTransfer().awaitInitialTransfer(true);
      if (indexed) {
         builder.indexing()
               .index(Index.PRIMARY_OWNER)
               .autoConfig(true)
               .addProperty("infinispan.query.lucene.max-boolean-clauses", "1025");
      }

      RemoteCache<K, V> cache = testMethodRule.hotrod().withClientConfiguration(config).withServerConfiguration(builder).create();
      RemoteCacheManager remoteCacheManager = cache.getRemoteCacheManager();

      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", testMethodRule.getClass().getClassLoader()));
      metadataCache.putIfAbsent("sample_bank_account/bank.proto", schema);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      Exceptions.unchecked(() -> MarshallerRegistration.registerMarshallers(MarshallerUtil.getSerializationContext(remoteCacheManager)));

      return cache;
   }

   static <K, V> RemoteCache<K, V> createStatsEnabledCache(InfinispanServerTestMethodRule testMethodRule) {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).stateTransfer().awaitInitialTransfer(true).hash().numOwners(2);
      builder.jmxStatistics().enable();
      return testMethodRule.hotrod().withClientConfiguration(new ConfigurationBuilder()).withServerConfiguration(builder).create();
   }
}

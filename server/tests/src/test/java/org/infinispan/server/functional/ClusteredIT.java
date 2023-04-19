package org.infinispan.server.functional;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({
      // This needs to be first as it collects all metrics and all the tests below add a lot which due to inefficiencies
      // in small rye can be very very slow!
      RestMetricsResource.class,
      HotRodCacheOperations.class,
      RestOperations.class,
      RestRouter.class,
      RestServerResource.class,
      MemcachedOperations.class,
      HotRodAdmin.class,
      HotRodCounterOperations.class,
      HotRodMultiMapOperations.class,
      HotRodTransactionalCacheOperations.class,
      HotRodCacheQueries.class,
      HotRodCacheContinuousQueries.class,
      HotRodListenerWithDslFilter.class,
      IgnoreCaches.class,
      RestLoggingResource.class
})
public class ClusteredIT {

   @ClassRule
   public static final InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(2)
               .property("infinispan.query.lucene.max-boolean-clauses", "1025")
               .build();

   static <K, V> RemoteCache<K, V> createQueryableCache(InfinispanServerTestMethodRule testMethodRule, boolean indexed) {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.marshaller(new ProtoStreamMarshaller());

      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.encoding().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).stateTransfer().awaitInitialTransfer(true);
      if (indexed) {
         builder.indexing().enable()
               .storage(LOCAL_HEAP)
               .addIndexedEntity("sample_bank_account.User");
      }

      HotRodTestClientDriver hotRodTestClientDriver = testMethodRule.hotrod().withClientConfiguration(config);
      RemoteCacheManager remoteCacheManager = hotRodTestClientDriver.createRemoteCacheManager();

      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", testMethodRule.getClass().getClassLoader()));
      metadataCache.putIfAbsent("sample_bank_account/bank.proto", schema);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      assertNotNull(metadataCache.get("sample_bank_account/bank.proto"));

      Exceptions.unchecked(() -> MarshallerRegistration.registerMarshallers(MarshallerUtil.getSerializationContext(remoteCacheManager)));

      return hotRodTestClientDriver.withServerConfiguration(builder).create();
   }
}

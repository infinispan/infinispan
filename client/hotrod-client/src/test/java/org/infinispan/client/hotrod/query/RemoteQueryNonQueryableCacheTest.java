package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests for error reporting when trying to query caches that are not queryable, i.e., not storing protobuf
 * or java objects
 *
 * @since 11.0
 */
@Test(testName = "client.hotrod.query.RemoteQueryNonQueryableCacheTest", groups = "functional")
public class RemoteQueryNonQueryableCacheTest extends SingleHotRodServerTest {

   private static final String DEFAULT_CACHE = "default";
   private static final String INDEXED_CACHE = "indexed";
   private static final String PROTOBUF_CACHE = "protobuf";
   private static final String POJO_CACHE = "object";
   private static final String JSON_CACHE = "json";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createServerModeCacheManager(TestDomainSCI.INSTANCE, new ConfigurationBuilder());

      cm.defineConfiguration(DEFAULT_CACHE, new ConfigurationBuilder().build());
      cm.defineConfiguration(INDEXED_CACHE, createIndexedCache());
      cm.defineConfiguration(PROTOBUF_CACHE, createCache(MediaType.APPLICATION_PROTOSTREAM_TYPE));
      cm.defineConfiguration(POJO_CACHE, createCache(MediaType.APPLICATION_OBJECT_TYPE));
      cm.defineConfiguration(JSON_CACHE, createCache(MediaType.APPLICATION_JSON_TYPE));
      return cm;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDataSCI.INSTANCE;
   }

   private Configuration createCache(String mediaType) {
      return new ConfigurationBuilder().encoding().mediaType(mediaType).build();
   }

   private Configuration createIndexedCache() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("sample_bank_account.User")
            .addIndexedEntity("sample_bank_account.Account")
            .addIndexedEntity("sample_bank_account.Transaction");
      return builder.build();
   }

   @Test
   public void testQueryable() {
      executeQuery(DEFAULT_CACHE);
      executeQuery(INDEXED_CACHE);
      executeQuery(PROTOBUF_CACHE);
      executeQuery(POJO_CACHE);
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028015.*")
   public void assertErrorForCacheWithoutNonQueryableEncoding() {
      executeQuery(JSON_CACHE);
   }

   private void executeQuery(String cacheName) {
      RemoteCache<String, User> remoteCache = remoteCacheManager.getCache(cacheName);
      Query<User> q = Search.getQueryFactory(remoteCache).create("FROM sample_bank_account.User");
      q.execute();
   }
}

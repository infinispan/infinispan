package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Person;
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

   private RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createServerModeCacheManager(TestDataSCI.INSTANCE, new ConfigurationBuilder());

      cm.defineConfiguration(DEFAULT_CACHE, new ConfigurationBuilder().build());
      cm.defineConfiguration(INDEXED_CACHE, createIndexedCache());
      cm.defineConfiguration(PROTOBUF_CACHE, createCache(MediaType.APPLICATION_PROTOSTREAM_TYPE));
      cm.defineConfiguration(POJO_CACHE, createCache(MediaType.APPLICATION_OBJECT_TYPE));
      cm.defineConfiguration(JSON_CACHE, createCache(MediaType.APPLICATION_JSON_TYPE));

      HotRodServer hotRodServer = HotRodClientTestingUtil.startHotRodServer(cm);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort()).addContextInitializers(TestDataSCI.INSTANCE);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      return cm;
   }

   private Configuration createCache(String mediaType) {
      return new ConfigurationBuilder().encoding().mediaType(mediaType).build();
   }

   private Configuration createIndexedCache() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().addProperty("default.directory_provider", "local-heap");
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
      RemoteCache<String, Person> remoteCache = remoteCacheManager.getCache(cacheName);
      remoteCache.put("1", new Person("John"));
      Query<Person> q = Search.getQueryFactory(remoteCache).create("FROM org.infinispan.test.core.Person");
      q.execute();
   }
}

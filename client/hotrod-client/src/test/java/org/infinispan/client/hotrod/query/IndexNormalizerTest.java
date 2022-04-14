package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.query.testdomain.protobuf.Book;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.IndexNormalizerTest")
public class IndexNormalizerTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("Book");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      manager.defineConfiguration("books", builder.build());
      return manager;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Book.BookSchema.INSTANCE;
   }

   @Test
   public void testLowercaseNormalizer() {
      RemoteCache<Integer, Book> remoteCache = remoteCacheManager.getCache("books");

      remoteCache.put(1, new Book("LIBERTY"));

      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);
      assertEquals(1, queryFactory.create("from Book where title : 'li*ty'").execute().hitCount().orElse(-1));
   }
}

package org.infinispan.client.hotrod.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.query.testdomain.protobuf.KeywordEntity;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.QueryLimitTest")
public class QueryLimitTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("KeywordEntity");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      manager.defineConfiguration("keyword", builder.build());
      return manager;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return KeywordEntity.KeywordSchema.INSTANCE;
   }

   @Test
   public void testNextPageWithNoMaxResults() {
      RemoteCache<Integer, KeywordEntity> remoteCache = remoteCacheManager.getCache("keyword");
      for (int i=0; i<20; i++) {
         remoteCache.put(i, new KeywordEntity(i + ""));
      }

      Query<KeywordEntity> query = remoteCache.query("from KeywordEntity");
      query.startOffset(10);
      query.maxResults(-1);

      QueryResult<KeywordEntity> result = query.execute();
      assertThat(result.count().value()).isEqualTo(20);
      assertThat(result.list()).hasSize(10);
   }
}

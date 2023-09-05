package org.infinispan.query.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Book;
import org.infinispan.query.model.Game;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.query.encoding.ProtobufEncodedIndexedCacheTest")
public class ProtobufEncodedIndexedCacheTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder
            .encoding()
               .mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
            .indexing()
               .enable()
               .storage(LOCAL_HEAP)
               .addIndexedEntity("org.infinispan.query.model.Game");

      cacheManager = TestCacheManagerFactory.createCacheManager(Game.GameSchema.INSTANCE, null);
      cache = cacheManager.administration()
         .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
         .getOrCreateCache("default", builder.build());
      return cacheManager;
   }

   @Test
   public void test() {
      cache.put(1, new Game("Civilization 1", "The best video game of all time!")); // according to the contributor

      Query<Book> query = cache.query("from org.infinispan.query.model.Game where description : 'game'");
      QueryResult<Book> result = query.execute();

      assertThat(result.count().isExact()).isTrue();
      assertThat(result.count().value()).isEqualTo(1);
      assertThat(result.list()).extracting("name").contains("Civilization 1");
   }
}

package org.infinispan.query.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.model.Item;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.vector.VectorSearchBroadcastTest")
public class VectorSearchBroadcastTest extends MultipleCacheManagersTest {

   private Cache<Object, Object> cache;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder indexed = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Item.class);

      createClusteredCaches(3, indexed);
      cache = cache(0);
   }

   @Test
   public void test() {
      for (byte item = 1; item <= 10; item++) {
         byte[] bytes = {item, item, item};
         cache.put(item, new Item("c" + item, bytes, new float[]{1.1f * item, 1.1f * item, 1.1f * item}, "bla" + item));
      }

      Query<Item> query = cache.query("from org.infinispan.query.model.Item i where i.byteVector <-> [7,6,7]~7");
      query.maxResults(3);
      List<Item> hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c6", "c8");

      query = cache.query("from org.infinispan.query.model.Item i where i.byteVector <-> [7,6,7]~7");
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c6", "c8", "c5", "c9", "c4", "c10");
   }
}

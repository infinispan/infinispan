package org.infinispan.query.projection;

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

public class ScoreProjectionBroadcastTest extends MultipleCacheManagersTest {

   private Cache<Object, Object> cache;

   @Override
   protected void createCacheManagers() {
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
         cache.put(item, new Item("c" + item, bytes, new float[]{1.1f * item, 1.1f * item, 1.1f * item}, "bla" + item, (int)item, null));
      }

      Query<Object[]> query;
      List<Object[]> hits;

      query = cache.query("select i, score(i) from org.infinispan.query.model.Item i where i.byteVector <-> [7,6,7]~7");
      query.maxResults(3);
      hits = query.list();
      assertThat(hits).extracting(objects -> objects[0]).extracting("code").containsExactly("c7", "c6", "c8");
      assertThat(hits).extracting(objects -> objects[1]).hasOnlyElementsOfType(Float.class).isNotNull().allMatch(o -> !o.equals(Float.NaN));

      query = cache.query("select i, score(i) from org.infinispan.query.model.Item i where i.byteVector <-> [7,6,7]~3 order by i.ordinal");
      query.maxResults(3);
      hits = query.list();
      assertThat(hits).extracting(objects -> objects[0]).extracting("code").containsExactly("c5", "c6", "c7");
      assertThat(hits).extracting(objects -> objects[1]).hasOnlyElementsOfType(Float.class).isNotNull().allMatch(o -> !o.equals(Float.NaN));


   }
}

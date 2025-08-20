package org.infinispan.query.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Item;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.vector.VectorSearchTest")
public class VectorSearchTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Item.class);

      return TestCacheManagerFactory.createCacheManager(indexed);
   }

   @Test
   public void test() {
      for (byte item = 1; item <= 10; item++) {
         byte[] bytes = {item, item, item};
         cache.put(item, new Item("c" + item, bytes, new float[]{1.1f * item, 1.1f * item, 1.1f * item}, "bla" + item, (int)item, null));
      }

      Query<Item> query = cache.query("from org.infinispan.query.model.Item i where i.byteVector <-> [7,7,7]~3");
      List<Item> hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c6", "c8");

      query = cache.query("from org.infinispan.query.model.Item i where i.floatVector <-> [7.1,7,3.1]~3");
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c5", "c6", "c4");

      query = cache.query("from org.infinispan.query.model.Item i where i.byteVector <-> [:a,:b,:c]~3");
      query.setParameter("a", 0);
      query.setParameter("b", 2);
      query.setParameter("c", 3);
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c2", "c1", "c3"); // the order matters

      query = cache.query("from org.infinispan.query.model.Item i where i.floatVector <-> [:a,:b,:c]~:d");
      query.setParameter("a", 1);
      query.setParameter("b", 4.3);
      query.setParameter("c", 3.3);
      query.setParameter("d", 4);
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c3", "c2", "c4", "c1");

      query = cache.query("from org.infinispan.query.model.Item i where i.byteVector <-> [:a]~3");
      query.setParameter("a", new byte[]{7, 7, 7});
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c6", "c8");

      query = cache.query("from org.infinispan.query.model.Item i where i.floatVector <-> [:a]~:b");
      query.setParameter("a", new float[]{7.1f, 7.0f, 3.1f});
      query.setParameter("b", 3);
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c5", "c6", "c4");
   }
}

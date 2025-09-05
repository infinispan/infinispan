package org.infinispan.query.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.model.Item;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.vector.VectorSearchBroadcastTest")
public class VectorSearchBroadcastTest extends MultipleCacheManagersTest {

   private static final String[] BUGGY_OPTIONS =
         {"cat lover", "code lover", "mystical", "philologist", "algorithm designer", "decisionist", "philosopher"};

   private Cache<Object, Object> cache;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder indexed = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Item.class);

      createClusteredCaches(3, Item.ItemSchema.INSTANCE, indexed);
      cache = cache(0);
   }

   @BeforeMethod
   public void populateCache() {
      for (byte item = 1; item <= 50; item++) {
         byte[] bytes = {item, item, item};
         String buggy = BUGGY_OPTIONS[item % 7];
         cache.put(item, new Item("c" + item, bytes, new float[]{1.1f * item, 1.1f * item, 1.1f * item}, buggy, (int)item, null));
      }
   }

   @Test
   public void test() {
      Query<Item> query = cache.query("from org.infinispan.query.model.Item i where i.byteVector <-> [7,6,7]~7");
      query.maxResults(3);
      List<Item> hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c6", "c8");

      query = cache.query("from org.infinispan.query.model.Item i where i.byteVector <-> [7,6,7]~7");
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c6", "c8", "c5", "c9", "c4", "c10");

      query = cache.query("from org.infinispan.query.model.Item i where i.byteVector <-> [:a]~3");
      query.setParameter("a", new byte[]{7, 7, 6});
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c6", "c8");

      query = cache.query("from org.infinispan.query.model.Item i where i.floatVector <-> [:a]~:b");
      query.setParameter("a", new float[]{7.1f, 7.0f, 3.1f});
      query.setParameter("b", 3);
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c5", "c6", "c4");
   }

   @Test
   public void withScore() {
      Query<Object[]> query = cache.query("select score(i), i from org.infinispan.query.model.Item i where i.byteVector <-> [7,6,7]~3");
      List<Object[]> hits = query.list();
      assertThat(hits).extracting(objects -> objects[0]).hasOnlyElementsOfType(Float.class).isNotNull().allMatch(o -> !o.equals(Float.NaN));
      assertThat(hits).extracting(objects -> objects[1]).extracting("code").containsExactly("c7", "c6", "c8");
   }

   @Test
   public void withScore_andKParam() {
      Query<Object[]> query = cache.query("select score(i), i from org.infinispan.query.model.Item i where i.byteVector <-> [7,6,7]~:k");
      query.setParameter("k", 3);
      List<Object[]> hits = query.list();
      assertThat(hits).extracting(objects -> objects[0]).hasOnlyElementsOfType(Float.class).isNotNull().allMatch(o -> !o.equals(Float.NaN));
      assertThat(hits).extracting(objects -> objects[1]).extracting("code").containsExactly("c7", "c6", "c8");
   }

   @Test
   @TestForIssue(jiraKey = "ISPN-15952")
   public void withScore_andVectorParam() {
      Query<Object[]> query = cache.query("select score(i), i from org.infinispan.query.model.Item i where i.byteVector <-> [:v]~:k");
      query.setParameter("k", 3);
      query.setParameter("v", new byte[]{7,6,7});
      List<Object[]> hits = query.list();
      assertThat(hits).extracting(objects -> objects[0]).hasOnlyElementsOfType(Float.class).isNotNull().allMatch(o -> !o.equals(Float.NaN));
      assertThat(hits).extracting(objects -> objects[1]).extracting("code").containsExactly("c7", "c6", "c8");
   }

   @Test
   public void withScore_andVectorCellParams() {
      Query<Object[]> query = cache.query("select score(i), i from org.infinispan.query.model.Item i where i.byteVector <-> [:a,:b,:c]~:k");
      query.setParameter("k", 3);
      query.setParameter("a", 7);
      query.setParameter("b", 6);
      query.setParameter("c", 7);
      List<Object[]> hits = query.list();
      assertThat(hits).extracting(objects -> objects[0]).hasOnlyElementsOfType(Float.class).isNotNull().allMatch(o -> !o.equals(Float.NaN));
      assertThat(hits).extracting(objects -> objects[1]).extracting("code").containsExactly("c7", "c6", "c8");
   }

   @Test
   public void ickleQuery_simpleFiltering() {
      Query<Object[]> query = cache.query(
            "select score(i), i from org.infinispan.query.model.Item i where i.floatVector <-> [:a]~:k filtering i.buggy : 'cat'");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      List<Object[]> hits = query.list();
      assertThat(hits).extracting(objects -> objects[1])
            .extracting("code").containsExactly("c7", "c14", "c21");
   }

   @Test
   public void ickleQuery_complexFiltering() {
      Query<Object[]> query = cache.query(
            "select score(i), i from org.infinispan.query.model.Item i where i.floatVector <-> [:a]~:k filtering (i.buggy : 'cat' or i.buggy : 'code')");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      List<Object[]> hits = query.list();
      assertThat(hits).extracting(objects -> objects[1])
            .extracting("code").containsExactly("c7", "c8", "c1");
   }

   @Test
   public void entityProjection() {
      Query<Item> query = cache.query(
            "from org.infinispan.query.model.Item i where i.floatVector <-> [:a]~:k filtering i.buggy : 'cat'");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      List<Item> hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c14", "c21");

      query = cache.query(
            "from org.infinispan.query.model.Item i where i.floatVector <-> [:a]~:k filtering (i.buggy : 'cat' or i.buggy : 'code')");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c8", "c1");
   }
}

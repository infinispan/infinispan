package org.infinispan.client.hotrod.query.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.model.Item;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.vector.VectorSearchRemoteTest")
public class VectorSearchRemoteTest extends SingleHotRodServerTest {

   private static final String[] BUGGY_OPTIONS =
         {"cat lover", "code lover", "mystical", "philologist", "algorithm designer", "decisionist", "philosopher"};

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("Item");

      return TestCacheManagerFactory.createServerModeCacheManager(contextInitializer(), indexed);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Item.ItemSchema.INSTANCE;
   }

   @BeforeMethod
   public void beforeClass() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      for (byte item = 1; item <= 50; item++) {
         byte[] bytes = {item, item, item};
         String buggy = BUGGY_OPTIONS[item % 7];
         remoteCache.put(item, new Item("c" + item, bytes, new float[]{1.1f * item, 1.1f * item, 1.1f * item}, buggy, (int) item, null));
      }
   }

   @Test
   public void test() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      Query<Item> query = remoteCache.query("from Item i where i.byteVector <-> [7,6,7]~3");
      List<Item> hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c6", "c8");

      query = remoteCache.query("from Item i where i.floatVector <-> [7.1,7,3.1]~3");
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c5", "c6", "c4");

      query = remoteCache.query("from Item i where i.byteVector <-> [:a]~:b");
      query.setParameter("a", new byte[]{7, 6, 7});
      query.setParameter("b", 3);
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c6", "c8");

      query = remoteCache.query("from Item i where i.floatVector <-> [:a]~:b");
      query.setParameter("a", new float[]{7.1f, 7.0f, 3.1f});
      query.setParameter("b", 3);
      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c5", "c6", "c4");

      Query<Object[]> scoreQuery = remoteCache.query("select score(i), i from Item i where i.byteVector <-> [7,6,7]~3");
      List<Object[]> scoreHits = scoreQuery.list();
      assertThat(scoreHits).extracting(objects -> objects[1]).extracting("code").containsExactly("c7", "c6", "c8");
      assertThat(scoreHits).extracting(objects -> objects[0]).hasOnlyElementsOfType(Float.class).isNotNull().allMatch(o -> !o.equals(Float.NaN));
   }

   @Test
   public void ickleQuery_simpleFiltering() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      Query<Object[]> query = remoteCache.query(
            "select score(i), i from Item i where i.floatVector <-> [:a]~:k filtering i.buggy : 'cat'");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      List<Object[]> hits = query.list();
      assertThat(hits).extracting(objects -> objects[1])
            .extracting("code").containsExactly("c7", "c14", "c21");
   }

   @Test
   public void ickleQuery_complexFiltering() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      Query<Object[]> query = remoteCache.query(
            "select score(i), i from Item i where i.floatVector <-> [:a]~:k filtering (i.buggy : 'cat' or i.buggy : 'code')");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      List<Object[]> hits = query.list();
      assertThat(hits).extracting(objects -> objects[1])
            .extracting("code").containsExactly("c7", "c8", "c1");
   }

   @Test
   public void entityProjection() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      Query<Item> query = remoteCache.query(
            "from Item i where i.floatVector <-> [:a]~:k filtering i.buggy : 'cat'");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      List<Item> hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c14", "c21");

      query = remoteCache.query(
            "from Item i where i.floatVector <-> [:a]~:k filtering (i.buggy : 'cat' or i.buggy : 'code')");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c8", "c1");

      query = remoteCache.query(
            "from Item i where i.floatVector <-> [:a]~:k filtering ordinal = :o");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);
      query.setParameter("o", 9);

      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c9");
   }
}

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
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.vector.VectorSearchRemoteTest")
public class VectorSearchRemoteTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("Item");

      return TestCacheManagerFactory.createServerModeCacheManager(indexed);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Item.ItemSchema.INSTANCE;
   }

   @Test
   public void test() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      for (byte item = 1; item <= 10; item++) {
         byte[] bytes = {item, item, item};
         remoteCache.put(item, new Item("c" + item, bytes, new float[]{1.1f * item, 1.1f * item, 1.1f * item}, "bla" + item));
      }

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
   }
}

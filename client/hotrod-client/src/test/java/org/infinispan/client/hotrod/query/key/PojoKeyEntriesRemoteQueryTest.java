package org.infinispan.client.hotrod.query.key;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.model.Structure;
import org.infinispan.client.hotrod.annotation.model.StructureKey;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.model.Item;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.key.PojoKeyEntriesRemoteQueryTest")
public class PojoKeyEntriesRemoteQueryTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("model.Structure");

      return TestCacheManagerFactory.createServerModeCacheManager(indexed);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Structure.StructureSchema.INSTANCE;
   }

   @BeforeMethod
   public void setUp() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      if (!remoteCache.isEmpty()) {
         return;
      }

      for (byte item = 1; item <= 10; item++) {
         StructureKey key = new StructureKey("z" + item, (int) item, item * item);
         remoteCache.put(key, new Structure("c" + item, "bla bla bla", (int)item, key));
      }
   }

   @Test
   public void test() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      Query<Item> query = remoteCache.query("from model.Structure s where s.nested.zone = 'z7'");
      List<Item> list = query.list();
      assertThat(list).extracting("code").containsExactly("c7");

      query = remoteCache.query("from model.Structure s where s.nested.column = 9");
      list = query.list();
      assertThat(list).extracting("code").containsExactly("c3");
   }
}

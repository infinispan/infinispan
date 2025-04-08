package org.infinispan.client.hotrod.query;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.query.testdomain.protobuf.KeywordEntity;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.LargeTermTest")
public class LargeTermTest extends SingleHotRodServerTest {

   public static final String DESCRIPTION = "foo bar% baz";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("KeywordEntity");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager(contextInitializer());
      manager.defineConfiguration("keyword", builder.build());
      return manager;
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      // Make the timeout longer
      return super.createHotRodClientConfigurationBuilder(host, serverPort).socketTimeout(15_000);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return KeywordEntity.KeywordSchema.INSTANCE;
   }

   @Test
   public void test() {
      RemoteCache<Integer, KeywordEntity> remoteCache = remoteCacheManager.getCache("keyword");

      assertThatThrownBy(() -> remoteCache.put(1, new KeywordEntity(createLargeDescription(3000))))
            .isInstanceOf(HotRodClientException.class)
            .hasMessageContaining("bytes can be at most 32766");

      // the server continue to work
      KeywordEntity entity = new KeywordEntity(createLargeDescription(1));
      remoteCache.put(1, entity);

      assertEquals(1, remoteCache.query("from KeywordEntity where keyword : 'foo bar0 baz'").execute().count().value());
   }

   public String createLargeDescription(int times) {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < times; i++) {
         String desc = DESCRIPTION.replace("%", i + "");
         builder.append(desc);
         if (i < times - 1) {
            builder.append(" ");
         }
      }
      return builder.toString();
   }
}

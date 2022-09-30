package org.infinispan.client.hotrod.size;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.model.Essay;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.size.HugeProtobufMessageTest")
public class HugeProtobufMessageTest extends SingleHotRodServerTest {

   public static final int SIZE = 68_000_000; // use something that is > 64M (67,108,864)

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      manager.defineConfiguration("homeworks", new ConfigurationBuilder().build());
      return manager;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Essay.EssaySchema.INSTANCE;
   }

   @Test
   public void testSearches() {
      RemoteCache<Integer, Essay> remoteCache = remoteCacheManager.getCache("homeworks");

      remoteCache.put(1, new Essay("my-very-extensive-essay", makeHugeString()));

      Essay essay = remoteCache.get(1);
      assertThat(essay).isNotNull();
   }

   private String makeHugeString() {
      char[] chars = new char[SIZE];
      for (int i = 0; i < SIZE; i++) {
         char delta = (char) (i % 20);
         chars[i] = (char) ('a' + delta);
      }

      return new String(chars);
   }
}

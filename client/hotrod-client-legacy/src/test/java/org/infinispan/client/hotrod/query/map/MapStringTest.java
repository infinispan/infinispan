package org.infinispan.client.hotrod.query.map;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.HashMap;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.model.Message;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.map.MapStringTest")
public class MapStringTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.statistics().enable();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("model.Message");
      return TestCacheManagerFactory.createServerModeCacheManager(indexed);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Message.MessageSchema.INSTANCE;
   }

   @Test
   public void test() {
      RemoteCache<String, Message> remoteCache = remoteCacheManager.getCache();
      if (!remoteCache.isEmpty()) {
         return;
      }

      Message message = new Message();
      HashMap<String, String> header = new HashMap<>();
      header.put("bla", "bla");
      message.setHeader(header);
      message.setBody("bla bla bla");
      remoteCache.put("bla", message);
   }
}

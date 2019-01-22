package org.infinispan.api.collections.reactive;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.Infinispan;
import org.infinispan.InfinispanClient;
import org.infinispan.api.impl.InfinispanClientImpl;
import org.infinispan.api.search.reactive.Searchable;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

import io.reactivex.Flowable;

@Test(groups = "functional", testName = "api.client.InfinispanClientTest")
public class InfinispanClientTest extends SingleHotRodServerTest {

   private Infinispan infinispan;

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      cacheManager.createCache("test", new ConfigurationBuilder().build());
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      /**
       * In real world example, we should only need to call this way
       * Infinispan infinispan = InfinispanClient.newInfinispan();
       */
      InfinispanClientImpl infinispanClientImpl = (InfinispanClientImpl) InfinispanClient.newInfinispan();
      infinispanClientImpl.setCacheManager(remoteCacheManager);
      infinispan = infinispanClientImpl;
   }

   @Test
   public void testReactiveCacheMethods() {
      ReactiveCache<Integer, String> cache = infinispan.getReactiveCache("test");
      await(cache.put(1, "hi"));

      String value = await(cache.get(1));
      assertEquals("hi", value);

      String getBeforePut = await(cache.getAndPut(1, "bye"));
      assertEquals("hi", getBeforePut);

      String value2 = await(cache.get(1));
      assertEquals("bye", value2);

      List<Map.Entry<Integer, String>> entries = Stream.of(new AbstractMap.SimpleImmutableEntry<>(2, "adios"),
            new AbstractMap.SimpleImmutableEntry<>(3, "agur"),
            new AbstractMap.SimpleImmutableEntry<>(4, "ciao")).collect(Collectors.toList());

      await(cache.putMany(Flowable.fromIterable(entries)));

      long size = await(cache.size());

      assertEquals(4, size);

      String removedValue = await(cache.getAndRemove(1));

      long sizeAfterGetAndRemove = await(cache.size());
      assertEquals("bye", removedValue);
      assertEquals(3, sizeAfterGetAndRemove);

      await(cache.remove(2));
      long sizeAfterRemove = await(cache.size());

      assertEquals(2, sizeAfterRemove);

      assertEquals(2, Flowable.fromPublisher(cache.getKeys()).count().blockingGet().intValue());
      assertEquals(2, Flowable.fromPublisher(cache.getValues()).count().blockingGet().intValue());

      await(cache.clear());
      long sizeAfterClear = await(cache.size());
      assertEquals(0, sizeAfterClear);
   }

   @Test
   public void testSearch() {
      ReactiveCache<Integer, String> cache = infinispan.getReactiveCache("test");

      List<Map.Entry<Integer, String>> entries = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(1, "bye"),
            new AbstractMap.SimpleImmutableEntry<>(2, "adios"),
            new AbstractMap.SimpleImmutableEntry<>(3, "agur"),
            new AbstractMap.SimpleImmutableEntry<>(4, "ciao")).collect(Collectors.toList());

      await(cache.putMany(Flowable.fromIterable(entries)));

      Searchable<String> searchable = cache.asSearchable();
//      searchable.find()
   }

}

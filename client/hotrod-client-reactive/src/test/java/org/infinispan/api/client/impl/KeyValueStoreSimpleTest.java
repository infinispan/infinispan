package org.infinispan.api.client.impl;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.api.ClientConfig;
import org.infinispan.api.Infinispan;
import org.infinispan.api.InfinispanClient;
import org.infinispan.api.collections.reactive.KeyValueEntry;
import org.infinispan.api.collections.reactive.KeyValueStore;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.reactivex.Flowable;

@Test(groups = "functional", testName = "org.infinispan.api.client.impl.KeyyValueStoreSimpleTest")
public class KeyValueStoreSimpleTest extends SingleHotRodServerTest {

   private Infinispan infinispan;

   private KeyValueStore<Integer, String> store;

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      cacheManager.administration().createCache("test", new org.infinispan.configuration.cache.ConfigurationBuilder().build());
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
      store = infinispan.getKeyValueStore("test");
   }

   @BeforeMethod
   public void clearStoreBeforeEachTest() {
      KeyValueStore<Integer, String> store = infinispan.getKeyValueStore("test");
      await(store.clear());
   }

   public void testConfiguration() {
      // TODO: new configuration API https://issues.jboss.org/browse/ISPN-9929
      ClientConfig clientConfig = new ClientConfigurationLoader.ConfigurationWrapper(new ConfigurationBuilder().build());
      Infinispan client = InfinispanClient.newInfinispan(clientConfig);
      assertNotNull(client);
   }

   public void testGetNoValue() {
      assertNull(await(store.get(0)));
   }

   public void testPutAndGetValue() {
      await(store.put(1, "hi"));
      assertEquals("hi", await(store.get(1)));
   }

   public void testGetAndPut() {
      await(store.put(1, "hi"));
      String getAndPutValue = await(store.getAndPut(1, "hola"));
      assertEquals("hi", getAndPutValue);
      assertEquals("hola", await(store.get(1)));
   }

   public void testPutMany() {
      List<KeyValueEntry<Integer, String>> entries = Stream.of(new KeyValueEntry<>(1, "adios"),
            new KeyValueEntry<>(2, "agur"),
            new KeyValueEntry<>(3, "ciao")).collect(Collectors.toList());

      await(store.putMany(Flowable.fromIterable(entries)));

      assertEquals("adios", await(store.get(1)));
      assertEquals("agur", await(store.get(2)));
      assertEquals("ciao", await(store.get(3)));
   }

   public void testEstimateSizeEmptyStore() {
      long estimatedSize = await(store.estimateSize());

      assertEquals(0, estimatedSize);
   }

   public void testEstimateSizeWithData() {
      for (int i = 0; i < 100; i++) {
         await(store.put(i, "" + i));
      }

      long estimatedSize = await(store.estimateSize());
      assertEquals(100, estimatedSize);
   }

   public void testGetAndRemoveNotExistingKeyValue() {
      String removed = await(store.getAndRemove(0));
      assertNull(removed);
   }

   public void testGetAndRemoveExistingKeyValue() {
      await(store.put(0, "hi"));
      String removed = await(store.getAndRemove(0));
      assertEquals("hi", removed);
      String getRemovedValue = await(store.get(0));
      assertNull(getRemovedValue);
   }

   public void testRemoveKeyNotExisting() {
      await(store.remove(0));
   }

   public void testRemoveKeyExisting() {
      await(store.put(0, "hola"));
      await(store.remove(0));
      String getRemovedValue = await(store.get(0));
      assertNull(getRemovedValue);
   }

   public void testEntries() {
      await(store.put(1, "adios"));
      await(store.put(2, "agur"));
      await(store.put(3, "ciao"));

      long count = Flowable.fromPublisher(store.entries()).count().blockingGet();

      assertEquals(3, count);
   }

   @Test
   public void search_api() {
      // TODO: Task new API https://issues.jboss.org/browse/ISPN-9924
      List<KeyValueEntry<Integer, String>> entries = Stream.of(
            new KeyValueEntry<>(1, "bye"),
            new KeyValueEntry<>(2, "adios"),
            new KeyValueEntry<>(3, "agur"),
            new KeyValueEntry<>(4, "ciao")).collect(Collectors.toList());

      await(store.putMany(Flowable.fromIterable(entries)));

//      store.findContinuously();
   }

}

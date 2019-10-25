package org.infinispan.api.client.impl;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.AbstractMap;

import org.infinispan.api.Infinispan;
import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.KeyValueStoreConfig;
import org.infinispan.api.reactive.WriteResult;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.reactivestreams.Publisher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.reactivex.Flowable;
import io.reactivex.subscribers.TestSubscriber;

@Test(groups = "functional", testName = "org.infinispan.api.client.impl.KeyyValueStoreSimpleTest")
public class KeyValueStoreTest extends SingleHotRodServerTest {

   public static final String CACHE_NAME = "test";
   private Infinispan infinispan;

   private KeyValueStore<Integer, String> store;

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      cacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(CACHE_NAME, new org.infinispan.configuration.cache.ConfigurationBuilder().build());
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      infinispan = new InfinispanClientImpl(remoteCacheManager);
      store = FunctionalTestUtils.await(infinispan.getKeyValueStore(CACHE_NAME, KeyValueStoreConfig.defaultConfig()));
   }

   @Override
   protected void teardown() {
      await(infinispan.stop());
      super.teardown();
   }

   @BeforeMethod
   public void clearStoreBeforeEachTest() {
      KeyValueStore<Integer, String> store = FunctionalTestUtils.await(infinispan.getKeyValueStore(CACHE_NAME, KeyValueStoreConfig.defaultConfig()));
      await(store.clear());
   }

   public void testGetNoValue() {
      assertNull(await(store.get(0)));
   }

   public void testCreate() {
      Boolean writeResult = await(store.insert(1, "hi"));
      assertTrue(writeResult);
      assertEquals("hi", await(store.get(1)));
      Boolean writeResult2 = await(store.insert(1, "hi"));
      assertFalse(writeResult2);
   }

   public void testSaveMany() {
      Publisher<WriteResult<Integer>> publisher = store.saveMany(Flowable.fromArray(new AbstractMap.SimpleEntry(1, "adios"),
            new AbstractMap.SimpleEntry(2, "agur"),
            new AbstractMap.SimpleEntry(3, "ciao")));

      TestSubscriber<WriteResult<Integer>> testSubscriber = new TestSubscriber<>();
      publisher.subscribe(testSubscriber);

      testSubscriber.awaitCount(3);

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
         await(store.save(i, "" + i));
      }

      long estimatedSize = await(store.estimateSize());
      assertEquals(100, estimatedSize);
   }

   public void testDeleteNotExisting() {
      await(store.delete(0));
   }

   public void testDeleteExisting() {
      await(store.save(0, "hola"));
      await(store.delete(0));
      String getRemovedValue = await(store.get(0));
      assertNull(getRemovedValue);
   }

   public void testKeys() {
      for (int i = 0; i < 100; i++) {
         await(store.save(i, "" + i));
      }

      TestSubscriber subscriber = new TestSubscriber();
      store.keys().subscribe(subscriber);

      subscriber.awaitCount(100);

      assertEquals(100, subscriber.valueCount());
   }

   public void testEntries() {
      for (int i = 0; i < 100; i++) {
         await(store.save(i, "" + i));
      }

      TestSubscriber subscriber = new TestSubscriber();
      store.entries().subscribe(subscriber);

      subscriber.awaitCount(100);

      assertEquals(100, subscriber.valueCount());
   }
}

package org.infinispan.api.client.impl;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.api.Infinispan;
import org.infinispan.api.client.listener.ClientKeyValueStoreListener;
import org.infinispan.api.reactive.EntryStatus;
import org.infinispan.api.reactive.KeyValueEntry;
import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.KeyValueStoreConfig;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.reactivex.subscribers.TestSubscriber;

@Test(groups = "functional", testName = "org.infinispan.api.client.impl.KeyyValueStoreSimpleTest")
public class ListenersKeyValueStoreTest extends SingleHotRodServerTest {

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

   @Test
   public void testListenAllEvents() {
      TestSubscriber<KeyValueEntry<Integer, String>> subscriber = new TestSubscriber<>();
      store.listen(ClientKeyValueStoreListener.create()).subscribe(subscriber);

      putData();
      await(store.save(3, "kaixito"));
      await(store.delete(4));
      subscriber.awaitCount(7);
      subscriber.assertValueCount(7);

      long created = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.CREATED).count();
      long updated = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.UPDATED).count();
      long deleted = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.DELETED).count();

      assertEquals(4, created);
      assertEquals(2, updated);
      assertEquals(1, deleted);
   }

   @Test
   public void testListenCreation() {
      TestSubscriber<KeyValueEntry<Integer, String>> subscriber = new TestSubscriber<>();
      store.listen(ClientKeyValueStoreListener.create(EntryStatus.CREATED)).subscribe(subscriber);
      putData();
      await(store.save(3, "kaixito"));
      await(store.delete(4));
      subscriber.awaitCount(4);
      subscriber.assertValueCount(4);

      long created = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.CREATED).count();
      long updated = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.UPDATED).count();
      long deleted = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.DELETED).count();

      assertEquals(4, created);
      assertEquals(0, updated);
      assertEquals(0, deleted);
   }

   @Test
   public void testListenUpdated() {
      TestSubscriber<KeyValueEntry<Integer, String>> subscriber = new TestSubscriber<>();
      store.listen(ClientKeyValueStoreListener.create(EntryStatus.UPDATED)).subscribe(subscriber);
      putData();
      await(store.save(3, "kaixito"));
      await(store.delete(4));
      subscriber.awaitCount(2);
      subscriber.assertValueCount(2);

      long created = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.CREATED).count();
      long updated = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.UPDATED).count();
      long deleted = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.DELETED).count();

      assertEquals(0, created);
      assertEquals(2, updated);
      assertEquals(0, deleted);
   }

   @Test
   public void testListenDeleted() {
      TestSubscriber<KeyValueEntry<Integer, String>> subscriber = new TestSubscriber<>();
      store.listen(ClientKeyValueStoreListener.create(EntryStatus.DELETED)).subscribe(subscriber);
      putData();
      await(store.save(3, "kaixito"));
      await(store.delete(4));
      subscriber.awaitCount(1);
      subscriber.assertValueCount(1);

      long created = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.CREATED).count();
      long updated = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.UPDATED).count();
      long deleted = subscriber.values().stream().filter(kv -> kv.status() == EntryStatus.DELETED).count();

      assertEquals(0, created);
      assertEquals(0, updated);
      assertEquals(1, deleted);
   }

   private void putData() {
      await(store.save(1, "hi"));
      await(store.save(2, "hola"));
      await(store.save(3, "hello"));
      await(store.save(4, "kaixo"));
      await(store.save(2, "holita"));
   }
}

package org.infinispan.api.client.impl;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.api.Infinispan;
import org.infinispan.api.InfinispanClient;
import org.infinispan.api.collections.reactive.KeyValueEntry;
import org.infinispan.api.collections.reactive.KeyValueStore;
import org.infinispan.api.collections.reactive.KeyValueStoreConfig;
import org.infinispan.api.search.reactive.QueryParameters;
import org.infinispan.api.search.reactive.QueryPublisher;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.reactivex.Flowable;
import io.reactivex.subscribers.TestSubscriber;

@Test(groups = "functional", testName = "org.infinispan.api.client.impl.KeyValueStoreSearchTest")
public class KeyValueStoreSearchTest extends SingleHotRodServerTest {

   private static final String PEOPLE = "people";
   private Infinispan infinispan;

   private KeyValueStore<Integer, Person> store;

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      cacheManager.administration().createCache(PEOPLE, new org.infinispan.configuration.cache.ConfigurationBuilder().build());
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
      KeyValueStoreConfig storeConfig = KeyValueStoreConfig.init(Person.class)
            .withPackageName("org.infinispan")
            .withSchemaFileName("persons");
      store = infinispan.getKeyValueStore(PEOPLE, storeConfig);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ClientConfigurationLoader.DEFAULT_CONFIGURATION_BUILDER
            .addServer().host("127.0.0.1").port(hotrodServer.getPort());
      return new InternalRemoteCacheManager(ClientConfigurationLoader.DEFAULT_CONFIGURATION_BUILDER.build());
   }

   @BeforeMethod
   public void clearStoreBeforeEachTest() {
      await(store.clear());
   }

   @Test
   public void search_api() {
      List<KeyValueEntry<Integer, Person>> entries = Stream.of(
            new KeyValueEntry<>(1, new Person("Sare", "Bilbao", 1984, "Barakaldo")),
            new KeyValueEntry<>(2, new Person("Daniela", "Aketxa", 1986, "Donosti")),
            new KeyValueEntry<>(3, new Person("Unai", "Bilbao", 1988, "Gazteiz")),
            new KeyValueEntry<>(4, new Person("Gorka", "Uriarte", 1990, "Paris")))
            .collect(Collectors.toList());

      await(store.putMany(Flowable.fromIterable(entries)));

      QueryPublisher<Person> queryPublisher = store.find();
      queryPublisher.query("FROM org.infinispan.Person p where p.lastName = :lastName",
            QueryParameters.init("lastName", "Bilbao"));

      TestSubscriber<Person> personTestSubscriber = new TestSubscriber<>();
      queryPublisher.subscribe(personTestSubscriber);

      assertEquals(2, personTestSubscriber.valueCount());
   }
}

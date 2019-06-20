package org.infinispan.api.client.impl;

import static org.infinispan.api.client.impl.SearchUtil.ELAIA;
import static org.infinispan.api.client.impl.SearchUtil.OIHANA;
import static org.infinispan.api.client.impl.SearchUtil.PEOPLE;
import static org.infinispan.api.client.impl.SearchUtil.populate;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.api.Infinispan;
import org.infinispan.api.reactive.ContinuousQueryPublisher;
import org.infinispan.api.reactive.KeyValueEntry;
import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.KeyValueStoreConfig;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.reactivex.subscribers.TestSubscriber;

@Test(groups = "functional", testName = "org.infinispan.api.client.impl.KeyValueStoreSearchTest")
public class ContinuousQueryKeyValueStoreTest extends SingleHotRodServerTest {

   private Infinispan infinispan;
   private KeyValueStore<String, Person> store;

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
      infinispan = new InfinispanClientImpl(remoteCacheManager);
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

   @BeforeMethod(alwaysRun = true)
   @Override
   protected void createBeforeMethod() throws Exception {
      populate(store);
   }

   @Test
   public void continuous_query_search() {
      ContinuousQueryPublisher<String, Person> continuousQueryPublisher = store.findContinuously();

      continuousQueryPublisher.query("FROM org.infinispan.Person p where p.address.number = :number")
            .withQueryParameter("number", "12");

      TestSubscriber<KeyValueEntry<String, Person>> personTestSubscriber = new TestSubscriber<>();
      continuousQueryPublisher.subscribe(personTestSubscriber);
      personTestSubscriber.assertValueCount(1);

      for (int i = 0; i < 10; i++) {
         Person person = new Person(OIHANA.firstName + i, OIHANA.lastName, OIHANA.bornYear, OIHANA.bornIn);
         person.address = OIHANA.address;
         await(store.put(SearchUtil.id(), person));
      }

      personTestSubscriber.assertValueCount(10);

      // Filter all "OIHANA"
      List<String> personNames = personTestSubscriber.values().stream()
            .map(KeyValueEntry::value)
            .map(Person::getFirstName)
            .filter(name -> name.contains(OIHANA.firstName))
            .collect(Collectors.toList());

      assertEquals(11, personNames.size());
   }

   @Test
   public void continuous_query_search_only_created_entries() {
      TestSubscriber<KeyValueEntry<String, Person>> personTestSubscriber = new TestSubscriber<>();
      store.findContinuously("FROM org.infinispan.Person p")
            .subscribe(personTestSubscriber);

      personTestSubscriber.assertSubscribed();

      updateOneValue();

      personTestSubscriber.assertComplete();
      personTestSubscriber.assertValueCount(6);
   }

   @Test
   public void continuous_query_search_updated_entries() throws Exception {
      ContinuousQueryPublisher<String, Person> continuousQueryPublisher = store.findContinuously();

      continuousQueryPublisher.query("FROM org.infinispan.Person p")
            .updated();

      TestSubscriber<KeyValueEntry<String, Person>> personTestSubscriber = new TestSubscriber<>();
      continuousQueryPublisher.subscribe(personTestSubscriber);
      personTestSubscriber.assertSubscribed();
      personTestSubscriber.assertValueCount(0);

      updateOneValue();

      personTestSubscriber.await(1, TimeUnit.SECONDS);
      await(store.remove(OIHANA.id));
      personTestSubscriber.assertValueCount(1);
   }

   @Test
   public void continuous_query_search_removed_entries() throws Exception {
      ContinuousQueryPublisher<String, Person> continuousQueryPublisher = store.findContinuously();

      continuousQueryPublisher.query("FROM org.infinispan.Person p")
            .removed();

      TestSubscriber<KeyValueEntry<String, Person>> personTestSubscriber = new TestSubscriber<>();
      continuousQueryPublisher.subscribe(personTestSubscriber);
      personTestSubscriber.assertSubscribed();
      personTestSubscriber.assertValueCount(0);

      await(store.clear());
      personTestSubscriber.await(1, TimeUnit.SECONDS);
      personTestSubscriber.assertValueCount(6);

      personTestSubscriber.assertComplete();
   }

   private void updateOneValue() {
      Person copied = OIHANA.copy();
      copied.setAddress(new Address("25", "c/ Trafalgar", "28990", "Madrid", "Spain"));
      await(store.remove(ELAIA.id));
      await(store.put(OIHANA.id, copied));
   }
}

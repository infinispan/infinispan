package org.infinispan.api.client.impl;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.api.Infinispan;
import org.infinispan.api.collections.reactive.KeyValueEntry;
import org.infinispan.api.collections.reactive.KeyValueStore;
import org.infinispan.api.collections.reactive.KeyValueStoreConfig;
import org.infinispan.api.search.reactive.ContinuousQueryPublisher;
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

   private KeyValueStore<String, Person> store;

   private Person sare;
   private Person daniela;
   private Person unai;
   private Person gorka;

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

   @BeforeMethod
   public void createData() {
      await(store.clear());
      sare = new Person("Sare", "Bilbao", 1984, "Barakaldo");
      daniela = new Person("Daniela", "Aketxa", 1986, "Donosti");
      unai = new Person("Unai", "Bilbao", 1988, "Gazteiz");
      gorka = new Person("Gorka", "Uriarte", 1990, "Paris");

      sare.setAddress(new Address("12", "rue des marguettes", "75011", "Paris", "France"));
      daniela.setAddress(new Address("187", "rue de charonne", "75011", "Paris", "France"));
      unai.setAddress(new Address("16", "rue de la py", "75019", "Paris", "France"));
      gorka.setAddress(new Address("26", "rue des marguettes", "75018", "Paris", "France"));

      List<KeyValueEntry<String, Person>> entries = Stream.of(
            new KeyValueEntry<>(id(), sare),
            new KeyValueEntry<>(id(), daniela),
            new KeyValueEntry<>(id(), unai),
            new KeyValueEntry<>(id(), gorka))
            .collect(Collectors.toList());

      await(store.putMany(Flowable.fromIterable(entries)));
   }

   @Test
   public void search_api() {
      QueryPublisher<Person> queryPublisher = store.find();
      queryPublisher.query("FROM org.infinispan.Person p where p.lastName = :lastName and p.address.number = :number")
            .withQueryParameters(QueryParameters.init("lastName", "Bilbao").append("number", "12"));

      TestSubscriber<Person> personTestSubscriber = new TestSubscriber<>();
      queryPublisher.subscribe(personTestSubscriber);

      assertEquals(1, personTestSubscriber.valueCount());

      Person person = personTestSubscriber.values().stream().findFirst().get();

      assertEquals(sare, person);
   }

   @Test
   public void continuous_query_search() {
      ContinuousQueryPublisher<String, Person> continuousQueryPublisher = store.findContinuously();

      continuousQueryPublisher.query("FROM org.infinispan.Person p where p.address.number = :number")
            .withQueryParameters(QueryParameters.init("number", "12"));

      TestSubscriber<KeyValueEntry<String, Person>> personTestSubscriber = new TestSubscriber<>();
      continuousQueryPublisher.subscribe(personTestSubscriber);

      assertEquals(1, personTestSubscriber.values().size());

      for (int i = 0; i < 10; i++) {
         Person person = new Person(sare.firstName + i, sare.lastName, sare.bornYear, sare.bornIn);
         person.address = sare.address;
         await(store.put(id(), person));
      }

      // Filter all "sare"
      List<String> personNames = personTestSubscriber.values().stream()
            .map(KeyValueEntry::getValue)
            .map(Person::getFirstName)
            .filter(name -> name.contains(sare.firstName))
            .collect(Collectors.toList());
      assertEquals(11, personNames.size());


      continuousQueryPublisher.dispose(personTestSubscriber);
   }

   private String id() {
      return UUID.randomUUID().toString();
   }
}

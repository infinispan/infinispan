package org.infinispan.api.client.impl;

import static org.infinispan.api.client.impl.SearchUtil.DANIELA;
import static org.infinispan.api.client.impl.SearchUtil.EDOIA;
import static org.infinispan.api.client.impl.SearchUtil.ELAIA;
import static org.infinispan.api.client.impl.SearchUtil.MIREN;
import static org.infinispan.api.client.impl.SearchUtil.OIHANA;
import static org.infinispan.api.client.impl.SearchUtil.PEOPLE;
import static org.infinispan.api.client.impl.SearchUtil.UNAI;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.api.Infinispan;
import org.infinispan.api.reactive.KeyValueEntry;
import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.KeyValueStoreConfig;
import org.infinispan.api.reactive.query.ContinuousQueryRequestBuilder;
import org.infinispan.api.reactive.query.QueryRequest;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.reactivestreams.Publisher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.reactivex.subscribers.TestSubscriber;

@Test(groups = "functional", testName = "org.infinispan.api.client.impl.KeyValueStoreSearchTest")
public class SearchKeyValueStoreTest extends SingleHotRodServerTest {

   private Infinispan infinispan;

   private KeyValueStore<String, Person> store;

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      cacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).getOrCreateCache(PEOPLE, hotRodCacheConfiguration().build());
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      infinispan = new InfinispanClientImpl(remoteCacheManager);
      KeyValueStoreConfig storeConfig = KeyValueStoreConfig.init(Person.class)
            .withPackageName("org.infinispan")
            .withSchemaFileName("persons");
      store = FunctionalTestUtils.await(infinispan.getKeyValueStore(PEOPLE, storeConfig));
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   protected void createBeforeMethod() throws Exception {
      SearchUtil.populate(store);
   }

   @Override
   protected void teardown() {
      await(infinispan.stop());
      super.teardown();
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      Configuration config = new ConfigurationBuilder()
            .marshaller(ProtoStreamMarshaller.class)
            .addServer().host("127.0.0.1").port(hotrodServer.getPort())
            .build();
      return new InternalRemoteCacheManager(config);
   }

   @Test
   public void continuous_query_search() {
      TestSubscriber<KeyValueEntry<String, Person>> personTestSubscriber = new TestSubscriber<>();
      QueryRequest queryRequest = ContinuousQueryRequestBuilder
            .query("FROM org.infinispan.Person p where p.address.number = :number")
            .param("number", 12)
            .all()
            .build();

      Publisher<KeyValueEntry<String, Person>> continuously = store.findContinuously(queryRequest);
      continuously.subscribe(personTestSubscriber);

      personTestSubscriber.awaitCount(1);
      personTestSubscriber.assertValueCount(1);

      for (int i = 0; i < 10; i++) {
         Person person = new Person(OIHANA.firstName + i, OIHANA.lastName, OIHANA.bornYear, OIHANA.bornIn);
         person.address = OIHANA.address;
         await(store.save(SearchUtil.id(), person));
      }

      personTestSubscriber.awaitCount(11);
      personTestSubscriber.assertValueCount(11);

      // Filter all "OIHANA"
      List<String> personNames = personTestSubscriber.values().stream()
            .map(KeyValueEntry::value)
            .map(Person::getFirstName)
            .filter(name -> name.contains(OIHANA.firstName))
            .collect(Collectors.toList());

      assertEquals(11, personNames.size());
   }

   @Test
   public void continuous_query_search_with_projection() {
      TestSubscriber<KeyValueEntry<String, Object[]>> personTestSubscriber = new TestSubscriber<>();
      QueryRequest queryRequest = ContinuousQueryRequestBuilder
            .query("SELECT firstName FROM org.infinispan.Person p")
            .created().build();

      Publisher<KeyValueEntry<String, Object[]>> continuously = store.findContinuously(queryRequest);
      continuously.subscribe(personTestSubscriber);

      personTestSubscriber.assertValueCount(6);
      List<String> firstNames = personTestSubscriber
            .values()
            .stream()
            .map(kv -> kv.value()).map(o -> (String) o[0])
            .collect(Collectors.toList());

      assertTrue(firstNames.contains(OIHANA.firstName));
      assertTrue(firstNames.contains(DANIELA.firstName));
      assertTrue(firstNames.contains(UNAI.firstName));
      assertTrue(firstNames.contains(ELAIA.firstName));
      assertTrue(firstNames.contains(MIREN.firstName));
      assertTrue(firstNames.contains(EDOIA.firstName));
   }

   @Test
   public void continuous_query_over() {
      TestSubscriber<KeyValueEntry<String, Person>> personTestSubscriber = new TestSubscriber<>();
      QueryRequest queryRequest = ContinuousQueryRequestBuilder.query("FROM org.infinispan.Person p")
            .created().build();

      Publisher<KeyValueEntry<String, Person>> continuously = store.findContinuously(queryRequest);

      continuously.subscribe(personTestSubscriber);

      personTestSubscriber.assertValueCount(6);
   }

   @Test
   public void continuous_query_search_only_created_entries() {
      TestSubscriber<KeyValueEntry<String, Person>> personTestSubscriber = new TestSubscriber<>();
      QueryRequest queryRequest = ContinuousQueryRequestBuilder.query("FROM org.infinispan.Person p")
            .created().build();

      Publisher<KeyValueEntry<String, Person>> continuously = store.findContinuously(queryRequest);
      continuously.subscribe(personTestSubscriber);


      personTestSubscriber.assertSubscribed();

      updateOneValue();

      personTestSubscriber.assertValueCount(6);
   }

   @Test
   public void continuous_query_search_updated_entries() throws Exception {
      TestSubscriber<KeyValueEntry<String, Person>> personTestSubscriber = new TestSubscriber<>();
      QueryRequest queryRequest = ContinuousQueryRequestBuilder.query("FROM org.infinispan.Person p")
            .updated().build();

      Publisher<KeyValueEntry<String, Person>> continuously = store.findContinuously(queryRequest);
      continuously.subscribe(personTestSubscriber);

      personTestSubscriber.assertSubscribed();
      personTestSubscriber.assertValueCount(0);

      updateOneValue();

      personTestSubscriber.await(1, TimeUnit.SECONDS);
      await(store.delete(OIHANA.id));
      personTestSubscriber.assertValueCount(1);
   }

   @Test
   public void continuous_query_search_deleted_entries() throws Exception {
      TestSubscriber<KeyValueEntry<String, Person>> personTestSubscriber = new TestSubscriber<>();
      QueryRequest queryRequest = ContinuousQueryRequestBuilder.query("FROM org.infinispan.Person p")
            .deleted().build();

      Publisher<KeyValueEntry<String, Person>> continuously = store.findContinuously(queryRequest);
      continuously.subscribe(personTestSubscriber);

      personTestSubscriber.assertSubscribed();
      personTestSubscriber.assertValueCount(0);

      await(store.clear());
      personTestSubscriber.await(1, TimeUnit.SECONDS);
      personTestSubscriber.assertValueCount(6);
   }

   private void updateOneValue() {
      Person copied = OIHANA.copy();
      copied.setAddress(new Address("25", "c/ Trafalgar", "28990", "Madrid", "Spain"));
      await(store.delete(ELAIA.id));
      await(store.save(OIHANA.id, copied));
   }
}

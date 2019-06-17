package org.infinispan.api.client.impl;

import static org.infinispan.api.client.impl.SearchUtil.EDOIA;
import static org.infinispan.api.client.impl.SearchUtil.MIREN;
import static org.infinispan.api.client.impl.SearchUtil.OIHANA;
import static org.infinispan.api.client.impl.SearchUtil.PEOPLE;
import static org.infinispan.api.client.impl.SearchUtil.UNAI;

import org.infinispan.api.Infinispan;
import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.KeyValueStoreConfig;
import org.infinispan.api.reactive.QueryParameters;
import org.infinispan.api.reactive.QueryPublisher;
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
public class SearchKeyValueStoreTest extends SingleHotRodServerTest {

   private Infinispan infinispan;

   private KeyValueStore<String, Person> store;

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      cacheManager.administration().getOrCreateCache(PEOPLE, new org.infinispan.configuration.cache.ConfigurationBuilder().build());
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

   @BeforeMethod(alwaysRun = true)
   @Override
   protected void createBeforeMethod() throws Exception {
      SearchUtil.populate(store);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ClientConfigurationLoader.DEFAULT_CONFIGURATION_BUILDER
            .addServer().host("127.0.0.1").port(hotrodServer.getPort());
      return new InternalRemoteCacheManager(ClientConfigurationLoader.DEFAULT_CONFIGURATION_BUILDER.build());
   }

   @Test
   public void search_api_with_params() throws Exception {
      TestSubscriber<Person> personTestSubscriber = new TestSubscriber<>();
      store.find()
            .query("FROM org.infinispan.Person p where p.lastName = :lastName and p.address.number = :number")
            .withQueryParameters(QueryParameters.init("lastName", "Bilbao").append("number", "12"))
            .subscribe(personTestSubscriber);

      personTestSubscriber.await();

      personTestSubscriber.assertComplete();
      personTestSubscriber.assertNoErrors();
      personTestSubscriber.assertResult(OIHANA);
   }

   @Test
   public void search_skip() throws Exception {
      QueryPublisher<Person> queryPublisher = store.find();
      queryPublisher.query("FROM org.infinispan.Person p where p.lastName = :lastName order by p.firstName")
            .withQueryParameter("lastName", "Bilbao");

      TestSubscriber<Person> personTestSubscriber = new TestSubscriber<>();
      queryPublisher.subscribe(personTestSubscriber);
      personTestSubscriber.await();

      personTestSubscriber.assertComplete();
      personTestSubscriber.assertNoErrors();
      personTestSubscriber.assertResult(MIREN, OIHANA, UNAI);
   }

   @Test
   public void search_limit() throws Exception {
      QueryPublisher<Person> queryPublisher = store.find();
      queryPublisher.query("FROM org.infinispan.Person p where p.lastName = :lastName order by p.firstName")
            .withQueryParameter("lastName", "Bilbao");

      TestSubscriber<Person> personTestSubscriber = new TestSubscriber<>();
      queryPublisher.subscribe(personTestSubscriber);
      personTestSubscriber.await();

      personTestSubscriber.assertComplete();
      personTestSubscriber.assertNoErrors();
      personTestSubscriber.assertResult(EDOIA);
   }
}

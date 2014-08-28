package org.infinispan.client.hotrod.query;

import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.hibernate.search.indexes.impl.IndexManagerHolder;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.query.RemoteQueryFactory;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.ModelFactoryPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.dsl.embedded.QueryDslConditionsTest;
import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.ModelFactory;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.*;

/**
 * Test for query conditions (filtering). Exercises the whole query DSL on the sample domain model.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryDslConditionsTest")
public class RemoteQueryDslConditionsTest extends QueryDslConditionsTest {

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;
   protected Cache<Object, Object> cache;

   @Override
   protected QueryFactory getQueryFactory() {
      return Search.getQueryFactory(remoteCache);
   }

   @Override
   protected ModelFactory getModelFactory() {
      return ModelFactoryPB.INSTANCE;
   }

   @Override
   protected RemoteCache<Object, Object> getCacheForQuery() {
      return remoteCache;
   }

   protected Cache<Object, Object> getEmbeddedCache() {
      return cache;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getConfigurationBuilder();
      createClusteredCaches(1, cfg);

      cache = manager(0).getCache();

      hotRodServer = TestHelper.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.read(Util.getResourceAsStream("/sample_bank_account/bank.proto", getClass().getClassLoader())));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return builder;
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   @Override
   public void testIndexPresence() {
      SearchFactoryImplementor searchFactory = (SearchFactoryImplementor) org.infinispan.query.Search.getSearchManager(cache).getSearchFactory();
      IndexManagerHolder indexManagerHolder = searchFactory.getIndexManagerHolder();

      assertTrue(searchFactory.getIndexedTypes().contains(ProtobufValueWrapper.class));
      assertNotNull(indexManagerHolder.getIndexManager(ProtobufValueWrapper.class.getName()));
   }

   @Override
   public void testQueryFactoryType() {
      assertEquals(RemoteQueryFactory.class, getQueryFactory().getClass());
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalArgumentException: Field notes from type sample_bank_account.User is not indexed")
   @Override
   public void testEqNonIndexed() throws Exception {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("notes").eq("Lorem ipsum dolor sit amet")
            .toBuilder().build();

      q.list();
   }

   @Test(enabled = false, expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*HQLLUCN000005:.*", description = "see https://issues.jboss.org/browse/ISPN-4423")
   @Override
   public void testInvalidEmbeddedAttributeQuery() throws Exception {
      QueryFactory qf = getQueryFactory();

      QueryBuilder queryBuilder = qf.from(getModelFactory().getUserImplClass())
            .setProjection("addresses");

      Query q = queryBuilder.build();

      //todo [anistor] it would be best if the problem would be detected early at build() instead at doing it at list()
      q.list();  // exception expected
   }

   /**
    * This test is overridden because dates need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testSampleDomainQuery9() throws Exception {
      QueryFactory qf = getQueryFactory();

      // all the transactions that happened in January 2013, projected by date field only
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .setProjection("date")
            .having("date").between(makeDate("2013-01-01"), makeDate("2013-01-31"))
            .toBuilder().build();

      List<Object[]> list = q.list();
      assertEquals(4, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
      assertEquals(1, list.get(2).length);
      assertEquals(1, list.get(3).length);

      for (int i = 0; i < 4; i++) {
         Long d = (Long) list.get(i)[0];
         assertTrue(d <= makeDate("2013-01-31").getTime());
         assertTrue(d >= makeDate("2013-01-01").getTime());
      }
   }

   public void testDefaultValue() throws Exception {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getAccountImplClass()).orderBy("description", SortOrder.ASC).build();

      List<Account> list = q.list();
      assertEquals(3, list.size());
      assertEquals("Checking account", list.get(0).getDescription());
   }
}

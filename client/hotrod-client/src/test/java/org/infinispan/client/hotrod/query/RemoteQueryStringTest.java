package org.infinispan.client.hotrod.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.sampledomain.AnalyzerTestEntity;
import org.infinispan.protostream.sampledomain.FlightRoute;
import org.infinispan.protostream.sampledomain.NotIndexed;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.Account;
import org.infinispan.protostream.sampledomain.bank.Transaction;
import org.infinispan.protostream.sampledomain.bank.User;
import org.infinispan.query.dsl.embedded.QueryStringTest;
import org.infinispan.server.core.query.impl.GlobalContextInitializer;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test for query language in remote mode.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryStringTest")
public class RemoteQueryStringTest extends QueryStringTest {

   @ProtoSchema(
         includeClasses = AnalyzerTestEntity.class,
         schemaFileName = "test.client.RemoteQueryStringTest",
         schemaFilePath = "org/infinispan/client/hotrod",
         schemaPackageName = "sample_domain",
         service = false
   )
   interface SCI extends SerializationContextInitializer {
   }

   private static final SerializationContextInitializer CUSTOM_ANALYZER_SCI = new SCIImpl();

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;
   protected Cache<Object, Object> cache;

   @BeforeClass
   @Override
   protected void populateCache() throws Exception {
      super.populateCache();

      getCacheForWrite().put("analyzed1", new AnalyzerTestEntity("tested 123", 3));
      getCacheForWrite().put("analyzed2", new AnalyzerTestEntity("testing 1234", 3));
      getCacheForWrite().put("analyzed3", new AnalyzerTestEntity("xyz", null));
   }


   /**
    * Both populating the cache and querying are done via remote cache.
    */
   @Override
   protected RemoteCache<Object, Object> getCacheForQuery() {
      return remoteCache;
   }

   protected Cache<Object, Object> getEmbeddedCache() {
      return cache;
   }

   protected int getNodesCount() {
      return 1;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().clusteredDefault();
      globalBuilder.serialization().addContextInitializers(GlobalContextInitializer.INSTANCE, TestDomainSCI.INSTANCE, CUSTOM_ANALYZER_SCI);
      createClusteredCaches(getNodesCount(), globalBuilder, getConfigurationBuilder(), true);

      cache = manager(0).getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort())
            .addContextInitializers(TestDomainSCI.INSTANCE, CUSTOM_ANALYZER_SCI);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
   }

    protected ConfigurationBuilder getConfigurationBuilder() {
       ConfigurationBuilder builder = hotRodCacheConfiguration();
       builder.indexing().enable()
             .storage(LOCAL_HEAP)
             .addIndexedEntity(User.ENTITY_NAME)
             .addIndexedEntity(Account.ENTITY_NAME)
             .addIndexedEntity(Transaction.ENTITY_NAME)
             .addIndexedEntity("sample_domain.AnalyzerTestEntity")
             .addIndexedEntity("sample_domain.FlightRoute");
       return builder;
    }

    @Override
    protected String getUserTypeName() {
       return User.ENTITY_NAME;
    }

    @Override
    protected String getAccountTypeName() {
       return Account.ENTITY_NAME;
    }

    @Override
    protected String getAddressTypeName() {
       return "sample_domain.Address";
    }

    @Override
    protected String getTransactionTypeName() {
       return Transaction.ENTITY_NAME;
    }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      killServers(hotRodServer);
      hotRodServer = null;
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN014036: Prefix, wildcard or regexp queries cannot be fuzzy.*")
   @Override
   public void testFullTextWildcardFuzzyNotAllowed() {
      super.testFullTextWildcardFuzzyNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN028526: Invalid query.*")
   @Override
   public void testFullTextRegexpFuzzyNotAllowed() {
      super.testFullTextRegexpFuzzyNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN028522: .*property is analyzed.*")
   @Override
   public void testExactMatchOnAnalyzedFieldNotAllowed() {
      // exception is wrapped in HotRodClientException
      super.testExactMatchOnAnalyzedFieldNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN028521: .*unless the property is indexed and analyzed.*")
   @Override
   public void testFullTextTermOnNonAnalyzedFieldNotAllowed() {
      // exception is wrapped in HotRodClientException
      super.testFullTextTermOnNonAnalyzedFieldNotAllowed();
   }

   /**
    * This test is overridden because instants need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testInstant1() {
      Query<User> q = createQueryFromString("from " + getUserTypeName() + " u where u.creationDate = " + Instant.parse("2011-12-03T10:15:30Z").toEpochMilli());

      List<User> list = q.execute().list();
      assertEquals(3, list.size());
   }

   /**
    * This test is overridden because instants need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testInstant2() {
      Query<User> q = createQueryFromString("from " + getUserTypeName() + " u where u.passwordExpirationDate = " + Instant.parse("2011-12-03T10:15:30Z").toEpochMilli());

      List<User> list = q.execute().list();
      assertEquals(3, list.size());
   }

   public void testCustomFieldAnalyzer() {
      Query<AnalyzerTestEntity> q = createQueryFromString("from sample_domain.AnalyzerTestEntity where f1:'test'");

      List<AnalyzerTestEntity> list = q.execute().list();
      assertEquals(2, list.size());
   }

   @Override
   public void testEqNonIndexedType() {
      Query<NotIndexed> q = createQueryFromString("from sample_domain.NotIndexed where notIndexedField = 'testing 123'");

      List<NotIndexed> list = q.execute().list();
      assertEquals(1, list.size());
      assertEquals("testing 123", list.get(0).notIndexedField);
   }

   @Override
   public void testDeleteByQueryOnNonIndexedType() {
      getCacheForWrite().put("notIndexedToBeDeleted", new NotIndexed("testing delete"));

      Query<NotIndexed> select = createQueryFromString("FROM sample_domain.NotIndexed WHERE notIndexedField = 'testing delete'");
      QueryResult<NotIndexed> result = select.execute();
      assertThat(result.count().value()).isOne();
      assertThat(result.count().exact()).isTrue();

      Query<Transaction> delete = createQueryFromString("DELETE FROM sample_domain.NotIndexed WHERE notIndexedField = 'testing delete'");
      assertEquals(1, delete.executeStatement());

      result = select.execute();
      assertThat(result.count().value()).isZero();
      assertThat(result.count().exact()).isTrue();
   }

   @Override
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028526: Invalid query.*")
   public void testDeleteWithProjections() {
      super.testDeleteWithProjections();
   }

   @Override
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028526: Invalid query.*")
   public void testDeleteWithOrderBy() {
      super.testDeleteWithOrderBy();
   }

   @Override
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028526: Invalid query.*")
   public void testDeleteWithGroupBy() {
      super.testDeleteWithGroupBy();
   }

   @Override
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN014057: DELETE and UPDATE statements cannot use paging \\(firstResult/maxResults\\)")
   public void testDeleteWithPaging() {
      super.testDeleteWithPaging();
   }

   @Override
   public void testUpdateByQueryOnNonIndexedType() {
      getCacheForWrite().put("notIndexedToBeUpdated", new NotIndexed("testing update"));

      Query<NotIndexed> select = createQueryFromString("FROM sample_domain.NotIndexed WHERE notIndexedField = 'testing update'");
      QueryResult<NotIndexed> result = select.execute();
      assertThat(result.count().value()).isOne();
      assertThat(result.count().exact()).isTrue();

      Query<NotIndexed> update = createQueryFromString("UPDATE FROM sample_domain.NotIndexed SET notIndexedField = 'updated value' WHERE notIndexedField = 'testing update'");
      assertEquals(1, update.executeStatement());

      Query<NotIndexed> selectUpdated = createQueryFromString("FROM sample_domain.NotIndexed WHERE notIndexedField = 'updated value'");
      result = selectUpdated.execute();
      assertThat(result.count().value()).isOne();
      assertThat(result.count().exact()).isTrue();
   }

   @Override
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028526: Invalid query.*")
   public void testUpdateWithProjections() {
      super.testUpdateWithProjections();
   }

   @Override
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028526: Invalid query.*")
   public void testUpdateWithOrderBy() {
      super.testUpdateWithOrderBy();
   }

   @Override
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028526: Invalid query.*")
   public void testUpdateWithGroupBy() {
      super.testUpdateWithGroupBy();
   }

   @Override
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN014057: DELETE and UPDATE statements cannot use paging \\(firstResult/maxResults\\)")
   public void testUpdateWithPaging() {
      super.testUpdateWithPaging();
   }

   @Override
   public void testSpatialPredicate() {
      Query<FlightRoute> q = createQueryFromString("SELECT r.name" +
            " FROM sample_domain.FlightRoute r" +
            " WHERE r.start WITHIN CIRCLE(46.7716, 23.5895, 100)");

      List<FlightRoute> list = q.execute().list();
      assertEquals(1, list.size());

      q = createQueryFromString("SELECT r.name" +
            " FROM sample_domain.FlightRoute r" +
            " WHERE r.start WITHIN CIRCLE(46.7716, 23.5895, 100) AND r.start NOT WITHIN CIRCLE(46.7716, 23.5895, 10)");

      list = q.execute().list();
      assertEquals(0, list.size());
   }
}

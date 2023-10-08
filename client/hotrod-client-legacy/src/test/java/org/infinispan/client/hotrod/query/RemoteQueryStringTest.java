package org.infinispan.client.hotrod.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.time.Instant;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.marshall.NotIndexedSchema;
import org.infinispan.client.hotrod.marshall.SpatialSchema;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AnalyzerTestEntity;
import org.infinispan.client.hotrod.query.testdomain.protobuf.ModelFactoryPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.query.dsl.embedded.QueryStringTest;
import org.infinispan.query.dsl.embedded.testdomain.FlightRoute;
import org.infinispan.query.dsl.embedded.testdomain.ModelFactory;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.impl.GlobalContextInitializer;
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
         schemaFilePath = "proto/generated",
         schemaPackageName = "sample_bank_account",
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

   @Override
   protected ModelFactory getModelFactory() {
      return ModelFactoryPB.INSTANCE;
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
      globalBuilder.serialization().addContextInitializers(GlobalContextInitializer.INSTANCE, TestDomainSCI.INSTANCE, NotIndexedSchema.INSTANCE, SpatialSchema.INSTANCE, CUSTOM_ANALYZER_SCI);
      createClusteredCaches(getNodesCount(), globalBuilder, getConfigurationBuilder(), true);

      cache = manager(0).getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort())
            .addContextInitializers(TestDomainSCI.INSTANCE, NotIndexedSchema.INSTANCE, SpatialSchema.INSTANCE, CUSTOM_ANALYZER_SCI);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("sample_bank_account.User")
            .addIndexedEntity("sample_bank_account.Account")
            .addIndexedEntity("sample_bank_account.Transaction")
            .addIndexedEntity("sample_bank_account.AnalyzerTestEntity")
            .addIndexedEntity("sample_bank_account.FlightRoute");
      return builder;
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      killServers(hotRodServer);
      hotRodServer = null;
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN014036: Prefix, wildcard or regexp queries cannot be fuzzy.*")
   @Override
   public void testFullTextWildcardFuzzyNotAllowed() {
      super.testFullTextWildcardFuzzyNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028526: Invalid query.*")
   @Override
   public void testFullTextRegexpFuzzyNotAllowed() {
      super.testFullTextRegexpFuzzyNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028522: .*property is analyzed.*")
   @Override
   public void testExactMatchOnAnalyzedFieldNotAllowed() {
      // exception is wrapped in HotRodClientException
      super.testExactMatchOnAnalyzedFieldNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028521: .*unless the property is indexed and analyzed.*")
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
      Query<User> q = createQueryFromString("from " + getModelFactory().getUserTypeName() + " u where u.creationDate = " + Instant.parse("2011-12-03T10:15:30Z").toEpochMilli());

      List<User> list = q.execute().list();
      assertEquals(3, list.size());
   }

   /**
    * This test is overridden because instants need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testInstant2() {
      Query<User> q = createQueryFromString("from " + getModelFactory().getUserTypeName() + " u where u.passwordExpirationDate = " + Instant.parse("2011-12-03T10:15:30Z").toEpochMilli());

      List<User> list = q.execute().list();
      assertEquals(3, list.size());
   }

   public void testCustomFieldAnalyzer() {
      Query<AnalyzerTestEntity> q = createQueryFromString("from sample_bank_account.AnalyzerTestEntity where f1:'test'");

      List<AnalyzerTestEntity> list = q.execute().list();
      assertEquals(2, list.size());
   }

   @Override
   public void testEqNonIndexedType() {
      Query<NotIndexed> q = createQueryFromString("from sample_bank_account.NotIndexed where notIndexedField = 'testing 123'");

      List<NotIndexed> list = q.execute().list();
      assertEquals(1, list.size());
      assertEquals("testing 123", list.get(0).notIndexedField);
   }

   @Override
   public void testDeleteByQueryOnNonIndexedType() {
      getCacheForWrite().put("notIndexedToBeDeleted", new NotIndexed("testing delete"));

      Query<NotIndexed> select = createQueryFromString("FROM sample_bank_account.NotIndexed WHERE notIndexedField = 'testing delete'");
      QueryResult<NotIndexed> result = select.execute();
      assertThat(result.count().value()).isOne();
      assertThat(result.count().isExact()).isTrue();

      Query<Transaction> delete = createQueryFromString("DELETE FROM sample_bank_account.NotIndexed WHERE notIndexedField = 'testing delete'");
      assertEquals(1, delete.executeStatement());

      result = select.execute();
      assertThat(result.count().value()).isZero();
      assertThat(result.count().isExact()).isTrue();
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
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN014057: DELETE statements cannot use paging \\(firstResult/maxResults\\)")
   public void testDeleteWithPaging() {
      super.testDeleteWithPaging();
   }

   @Override
   public void testSpatialPredicate() {
      Query<FlightRoute> q = createQueryFromString("SELECT r.name" +
            " FROM sample_bank_account.FlightRoute r" +
            " WHERE r.start WITHIN CIRCLE(46.7716, 23.5895, 100)");

      List<FlightRoute> list = q.execute().list();
      assertEquals(1, list.size());

      q = createQueryFromString("SELECT r.name" +
            " FROM sample_bank_account.FlightRoute r" +
            " WHERE r.start WITHIN CIRCLE(46.7716, 23.5895, 100) AND r.start NOT WITHIN CIRCLE(46.7716, 23.5895, 10)");

      list = q.execute().list();
      assertEquals(0, list.size());
   }
}

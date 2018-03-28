package org.infinispan.client.hotrod.query;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.query.dsl.Expression.avg;
import static org.infinispan.query.dsl.Expression.count;
import static org.infinispan.query.dsl.Expression.max;
import static org.infinispan.query.dsl.Expression.min;
import static org.infinispan.query.dsl.Expression.param;
import static org.infinispan.query.dsl.Expression.sum;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.List;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.query.RemoteQueryFactory;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.ModelFactoryPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.NotIndexedMarshaller;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.dsl.embedded.QueryDslConditionsTest;
import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.ModelFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.impl.ProgrammaticSearchMappingProviderImpl;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Test for query conditions (filtering). Exercises the whole query DSL on the sample domain model.
 * Uses Protobuf marshalling and Protobuf doc annotations for configuring indexing.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryDslConditionsTest")
public class RemoteQueryDslConditionsTest extends QueryDslConditionsTest {

   private static final String NOT_INDEXED_PROTO_SCHEMA = "package sample_bank_account;\n" +
         "/* @Indexed(false) */\n" +
         "message NotIndexed {\n" +
         "\toptional string notIndexedField = 1;\n" +
         "}\n";

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
      createClusteredCaches(1, cfg, true);

      cache = manager(0).getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
      initProtoSchema(remoteCacheManager);
   }

   protected void initProtoSchema(RemoteCacheManager remoteCacheManager) throws IOException {
      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", loadSchema());
      metadataCache.put("not_indexed.proto", NOT_INDEXED_PROTO_SCHEMA);
      RemoteQueryTestUtils.checkSchemaErrors(metadataCache);

      //initialize client-side serialization context
      SerializationContext serCtx = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      MarshallerRegistration.registerMarshallers(serCtx);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("not_indexed.proto", NOT_INDEXED_PROTO_SCHEMA));
      serCtx.registerMarshaller(new NotIndexedMarshaller());
   }

   protected String loadSchema() throws IOException {
      return Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader());
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "local-heap")
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
      SearchIntegrator searchIntegrator = org.infinispan.query.Search.getSearchManager(cache).unwrap(SearchIntegrator.class);

      assertTrue(searchIntegrator.getIndexBindings().containsKey(ProtobufValueWrapper.INDEXING_TYPE));
      assertNotNull(searchIntegrator.getIndexManager(cache.getName() + ProgrammaticSearchMappingProviderImpl.INDEX_NAME_SUFFIX));
   }

   @Override
   public void testQueryFactoryType() {
      assertEquals(RemoteQueryFactory.class, getQueryFactory().getClass());
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028503:.*")
   @Override
   public void testInvalidEmbeddedAttributeQuery() {
      // the original exception gets wrapped in HotRodClientException
      super.testInvalidEmbeddedAttributeQuery();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN014027: The property path 'addresses.postCode' cannot be projected because it is multi-valued")
   @Override
   public void testRejectProjectionOfRepeatedProperty() {
      // the original exception gets wrapped in HotRodClientException
      super.testRejectProjectionOfRepeatedProperty();
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
            .select("date")
            .having("date").between(makeDate("2013-01-01"), makeDate("2013-01-31"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(4, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
      assertEquals(1, list.get(2).length);
      assertEquals(1, list.get(3).length);

      for (int i = 0; i < 4; i++) {
         Long d = (Long) list.get(i)[0];
         assertTrue(d.compareTo(makeDate("2013-01-31").getTime()) <= 0);
         assertTrue(d.compareTo(makeDate("2013-01-01").getTime()) >= 0);
      }
   }

   public void testDefaultValue() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getAccountImplClass()).orderBy("description", SortOrder.ASC).build();

      List<Account> list = q.list();
      assertEquals(3, list.size());
      assertEquals("Checking account", list.get(0).getDescription());
   }

   @Override
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN014026: The expression 'surname' must be part of an aggregate function or it should be included in the GROUP BY clause")
   public void testGroupBy3() {
      // the original exception gets wrapped in HotRodClientException
      super.testGroupBy3();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN014021: Queries containing grouping and aggregation functions must use projections.")
   @Override
   public void testGroupBy5() {
      // the original exception gets wrapped in HotRodClientException
      super.testGroupBy5();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: Aggregation SUM cannot be applied to property of type java.lang.String")
   public void testGroupBy6() {
      // the original exception gets wrapped in HotRodClientException
      super.testGroupBy6();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028515: Cannot have aggregate functions in the WHERE clause : SUM.")
   public void testGroupBy7() {
      // the original exception gets wrapped in HotRodClientException
      super.testGroupBy7();
   }

   /**
    * This test is overridden because dates need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testDateGrouping1() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("date")
            .having("date").between(makeDate("2013-02-15"), makeDate("2013-03-15"))
            .groupBy("date")
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(makeDate("2013-02-27").getTime(), list.get(0)[0]);
   }

   /**
    * This test is overridden because dates need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testDateGrouping2() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(count("date"), min("date"))
            .having("description").eq("Hotel")
            .groupBy("id")
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);
      assertEquals(makeDate("2013-02-27").getTime(), list.get(0)[1]);
   }

   /**
    * This test is overridden because dates need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testDateGrouping3() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(min("date"), count("date"))
            .having("description").eq("Hotel")
            .groupBy("id")
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(makeDate("2013-02-27").getTime(), list.get(0)[0]);
      assertEquals(1L, list.get(0)[1]);
   }

   /**
    * This test is overridden because dates need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testDuplicateDateProjection() throws Exception {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("id", "date", "date")
            .having("description").eq("Hotel")
            .build();
      List<Object[]> list = q.list();

      assertEquals(1, list.size());
      assertEquals(3, list.get(0).length);
      assertEquals(3, list.get(0)[0]);
      assertEquals(makeDate("2013-02-27").getTime(), list.get(0)[1]);
      assertEquals(makeDate("2013-02-27").getTime(), list.get(0)[2]);
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "ISPN014825: Query parameter 'param2' was not set")
   @Override
   public void testMissingParamWithParameterMap() {
      // exception message code is different because it is generated by a different logger
      super.testMissingParamWithParameterMap();
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "ISPN014825: Query parameter 'param2' was not set")
   @Override
   public void testMissingParam() {
      // exception message code is different because it is generated by a different logger
      super.testMissingParam();
   }

   /**
    * This test is overridden because dates need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testComplexQuery() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(avg("amount"), sum("amount"), count("date"), min("date"), max("accountId"))
            .having("isDebit").eq(param("param"))
            .orderBy(avg("amount"), SortOrder.DESC).orderBy(count("date"), SortOrder.DESC)
            .orderBy(max("amount"), SortOrder.ASC)
            .build();

      q.setParameter("param", true);

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(5, list.get(0).length);
      assertEquals(143.50909d, (Double) list.get(0)[0], 0.0001d);
      assertEquals(7893d, (Double) list.get(0)[1], 0.0001d);
      assertEquals(55L, list.get(0)[2]);
      assertEquals(Long.class, list.get(0)[3].getClass());
      assertEquals(makeDate("2013-01-01").getTime(), list.get(0)[3]);
      assertEquals(2, list.get(0)[4]);
   }

   /**
    * This test is overridden because dates need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testDateFilteringWithGroupBy() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("date")
            .having("date").between(makeDate("2013-02-15"), makeDate("2013-03-15"))
            .groupBy("date")
            .build();
      List<Object[]> list = q.list();

      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(Long.class, list.get(0)[0].getClass());
      assertEquals(makeDate("2013-02-27").getTime(), list.get(0)[0]);
   }

   /**
    * This test is overridden because dates need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testAggregateDate() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(count("date"), min("date"))
            .having("description").eq("Hotel")
            .groupBy("id")
            .build();
      List<Object[]> list = q.list();

      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);
      assertEquals(Long.class, list.get(0)[1].getClass());
      assertEquals(makeDate("2013-02-27").getTime(), list.get(0)[1]);
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN014023: Using the multi-valued property path 'addresses.street' in the GROUP BY clause is not currently supported")
   @Override
   public void testGroupByMustNotAcceptRepeatedProperty() {
      // the original exception gets wrapped in HotRodClientException
      super.testGroupByMustNotAcceptRepeatedProperty();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN014024: The property path 'addresses.street' cannot be used in the ORDER BY clause because it is multi-valued")
   @Override
   public void testOrderByMustNotAcceptRepeatedProperty() {
      // the original exception gets wrapped in HotRodClientException
      super.testOrderByMustNotAcceptRepeatedProperty();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028515: Cannot have aggregate functions in the WHERE clause : MIN.")
   @Override
   public void testRejectAggregationsInWhereClause() {
      // the original exception gets wrapped in HotRodClientException
      super.testRejectAggregationsInWhereClause();
   }
}

package org.infinispan.client.hotrod.query;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.Account;
import org.infinispan.protostream.sampledomain.bank.Transaction;
import org.infinispan.protostream.sampledomain.bank.User;
import org.infinispan.query.dsl.embedded.QueryConditionsTest;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryConditionsTest")
public class RemoteQueryConditionsTest extends QueryConditionsTest {

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;
   protected Cache<Object, Object> cache;

   protected ProtocolVersion getProtocolVersion() {
      return ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
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

   @Override
   protected String queryDate(Temporal temporal) {
      return Long.toString(toDate(temporal).getTime());
   }

   protected String instant(Instant instant) {
      return Long.toString(instant.toEpochMilli());
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().clusteredDefault();
      globalBuilder.serialization().addContextInitializers(TestDomainSCI.INSTANCE);
      createClusteredCaches(1, globalBuilder, getConfigurationBuilder(), true);

      cache = manager(0).getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort())
            .socketTimeout(10_000)
            .addContextInitializers(TestDomainSCI.INSTANCE);
      clientBuilder.version(getProtocolVersion());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
   }

    protected ConfigurationBuilder getConfigurationBuilder() {
       ConfigurationBuilder builder = hotRodCacheConfiguration();
       builder.indexing().enable()
             .storage(LOCAL_HEAP)
             .addIndexedEntity(User.ENTITY_NAME)
             .addIndexedEntity(Account.ENTITY_NAME)
             .addIndexedEntity(Transaction.ENTITY_NAME);
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

   @Override
   public void testIndexPresence() {
      SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);

      // we have indexing for remote query!
      assertNotNull(searchMapping.indexedEntity(User.ENTITY_NAME));
      assertNotNull(searchMapping.indexedEntity(Account.ENTITY_NAME));
      assertNotNull(searchMapping.indexedEntity(Transaction.ENTITY_NAME));

      // we have some indexes for this cache
      assertEquals(3, searchMapping.allIndexedEntities().size());
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028503:.*")
   @Override
   public void testInvalidEmbeddedAttributeQuery() {
      // the original exception gets wrapped in HotRodClientException
      super.testInvalidEmbeddedAttributeQuery();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN014027: The property path 'addresses.postCode' cannot be projected because it is multi-valued")
   @Override
   public void testRejectProjectionOfRepeatedProperty() {
      // the original exception gets wrapped in HotRodClientException
      super.testRejectProjectionOfRepeatedProperty();
   }

   @Override
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN014026: The expression 'surname' must be part of an aggregate function or it should be included in the GROUP BY clause")
   public void testGroupBy3() {
      // the original exception gets wrapped in HotRodClientException
      super.testGroupBy3();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN014021: Queries containing grouping and aggregation functions must use projections.")
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

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN028515: Cannot have aggregate functions in the WHERE clause : SUM.")
   public void testGroupBy7() {
      // the original exception gets wrapped in HotRodClientException
      super.testGroupBy7();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: ISPN014825: Query parameter 'param2' was not set")
   @Override
   public void testMissingParamWithParameterMap() {
      // exception message code is different because it is generated by a different logger
      super.testMissingParamWithParameterMap();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: ISPN014825: Query parameter 'param2' was not set")
   @Override
   public void testMissingParam() {
      // exception message code is different because it is generated by a different logger
      super.testMissingParam();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN014023: Using the multi-valued property path 'addresses.street' in the GROUP BY clause is not currently supported")
   @Override
   public void testGroupByMustNotAcceptRepeatedProperty() {
      // the original exception gets wrapped in HotRodClientException
      super.testGroupByMustNotAcceptRepeatedProperty();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN014024: The property path 'addresses.street' cannot be used in the ORDER BY clause because it is multi-valued")
   @Override
   public void testOrderByMustNotAcceptRepeatedProperty() {
      // the original exception gets wrapped in HotRodClientException
      super.testOrderByMustNotAcceptRepeatedProperty();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN028515: Cannot have aggregate functions in the WHERE clause : MIN.")
   @Override
   public void testRejectAggregationsInWhereClause() {
      // the original exception gets wrapped in HotRodClientException
      super.testRejectAggregationsInWhereClause();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.query.objectfilter.ParsingException: ISPN028505: Invalid numeric literal ''")
   public void testBetweenArgsAreComparable() {
      super.testBetweenArgsAreComparable();
   }

   @Test(expectedExceptions = HotRodClientException.class)
   public void testIn3() {
      super.testIn3();
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014803: Parameter name cannot be null or empty")
   public void testNullParamName() {
      super.testNullParamName();
   }

   /**
    * This test is overridden because dates need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testSampleDomainQuery9() throws Exception {
      // all the transactions that happened in January 2013, projected by date field only
      Query<Object[]> q = queryCache(String.format("SELECT date FROM %s WHERE date BETWEEN %d AND %d", TRANSACTION_TYPE, makeDate("2013-01-01").getTime(), makeDate("2013-01-31").getTime()));

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
}

package org.infinispan.it.marshalling.jboss;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.query.RemoteQueryFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.QueryDslConditionsTest;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Test for query conditions (filtering). Exercises the whole query DSL on the sample domain model.
 * Uses jboss-marshalling and Hibernate Search annotations for configuring indexing.
 *
 * @author anistor@redhat.com
 * @since 9.1
 */
@Test(groups = "functional", testName = "client.hotrod.query.JBMARRemoteQueryDslConditionsTest")
public class JBMARRemoteQueryDslConditionsTest extends QueryDslConditionsTest {

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;
   protected Cache<Object, Object> cache;

   @Override
   protected QueryFactory getQueryFactory() {
      return Search.getQueryFactory(remoteCache);
   }

   /**
    * Both populating the cache and querying are done via remote cache.
    */
   @Override
   protected RemoteCache<Object, Object> getCacheForQuery() {
      return remoteCache;
   }

   protected ProtocolVersion getProtocolVersion() {
      return ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
   }

   protected Cache<Object, Object> getEmbeddedCache() {
      return cache;
   }

   @Override
   protected void createCacheManagers() {
      GlobalConfigurationBuilder globalCfg = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalCfg.serialization().marshaller(new GenericJBossMarshaller());
      ConfigurationBuilder cfg = getConfigurationBuilder();
      createClusteredCaches(1, globalCfg, cfg, true);

      cache = manager(0).getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.version(getProtocolVersion());
      clientBuilder.marshaller(new GenericJBossMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
      cacheManagers.forEach(c -> c.getClassAllowList().addRegexps(".*"));
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      builder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      builder.indexing().enable()
             .addIndexedEntity(getModelFactory().getUserImplClass())
             .addIndexedEntity(getModelFactory().getAccountImplClass())
             .addIndexedEntity(getModelFactory().getTransactionImplClass())
             .addProperty("directory.type", "local-heap");
      return builder;
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

      verifyClassIsIndexed(searchMapping, getModelFactory().getUserImplClass());
      verifyClassIsIndexed(searchMapping, getModelFactory().getAccountImplClass());
      verifyClassIsIndexed(searchMapping, getModelFactory().getTransactionImplClass());
      verifyClassIsNotIndexed(searchMapping, getModelFactory().getAddressImplClass());
   }

   private void verifyClassIsNotIndexed(SearchMapping searchMapping, Class<?> type) {
      assertNull(searchMapping.indexedEntity(type));
   }

   private void verifyClassIsIndexed(SearchMapping searchMapping, Class<?> type) {
      assertNotNull(searchMapping.indexedEntity(type));
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

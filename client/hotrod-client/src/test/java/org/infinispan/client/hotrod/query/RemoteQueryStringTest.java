package org.infinispan.client.hotrod.query;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.time.Instant;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.marshall.NotIndexedSCI;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AnalyzerTestEntity;
import org.infinispan.client.hotrod.query.testdomain.protobuf.ModelFactoryPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.AnalyzerTestEntityMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.marshall.AbstractSerializationContextInitializer;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.QueryStringTest;
import org.infinispan.query.dsl.embedded.testdomain.ModelFactory;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;
import org.infinispan.query.dsl.embedded.testdomain.User;
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

   private static final SerializationContextInitializer CUSTOM_ANALYZER_SCI = new AbstractSerializationContextInitializer("custom_analyzer.proto") {
      @Override
      public String getProtoFile() {
         return "package sample_bank_account;\n" +
               "/* @Indexed \n" +
               "   @Analyzer(definition = \"standard-with-stop\") */" +
               "message AnalyzerTestEntity {\n" +
               "\t/* @Field(store = Store.YES, analyze = Analyze.YES, analyzer = @Analyzer(definition = \"stemmer\")) */\n" +
               "\toptional string f1 = 1;\n" +
               "\t/* @Field(store = Store.YES, analyze = Analyze.NO, indexNullAs = \"-1\") */\n" +
               "\toptional int32 f2 = 2;\n" +
               "}\n";
      }

      @Override
      public void registerMarshallers(SerializationContext serCtx) {
         serCtx.registerMarshaller(new AnalyzerTestEntityMarshaller());
      }
   };

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
   protected QueryFactory getQueryFactory() {
      return Search.getQueryFactory(remoteCache);
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
      globalBuilder.serialization().addContextInitializers(TestDomainSCI.INSTANCE, NotIndexedSCI.INSTANCE, CUSTOM_ANALYZER_SCI);
      createClusteredCaches(getNodesCount(), globalBuilder, getConfigurationBuilder(), true);

      cache = manager(0).getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort())
                   .addContextInitializers(TestDomainSCI.INSTANCE, NotIndexedSCI.INSTANCE, CUSTOM_ANALYZER_SCI);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.indexing().enable()
             .addIndexedEntity("sample_bank_account.User")
             .addIndexedEntity("sample_bank_account.Account")
             .addIndexedEntity("sample_bank_account.Transaction")
             .addIndexedEntity("sample_bank_account.AnalyzerTestEntity")
             .addProperty("default.directory_provider", "local-heap")
             .addProperty("lucene_version", "LUCENE_CURRENT");
      return builder;
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
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
}

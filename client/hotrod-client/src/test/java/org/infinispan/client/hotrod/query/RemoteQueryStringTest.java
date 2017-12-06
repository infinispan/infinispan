package org.infinispan.client.hotrod.query;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AnalyzerTestEntity;
import org.infinispan.client.hotrod.query.testdomain.protobuf.ModelFactoryPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.AnalyzerTestEntityMarshaller;
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
import org.infinispan.query.dsl.embedded.QueryStringTest;
import org.infinispan.query.dsl.embedded.testdomain.ModelFactory;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
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

   private static final String NOT_INDEXED_PROTO_SCHEMA = "package sample_bank_account;\n" +
         "/* @Indexed(false) */\n" +
         "message NotIndexed {\n" +
         "\toptional string notIndexedField = 1;\n" +
         "}\n";

   private static final String CUSTOM_ANALYZER_PROTO_SCHEMA = "package sample_bank_account;\n" +
         "/* @Indexed \n" +
         "   @Analyzer(definition = \"standard\") */" +
         "message AnalyzerTestEntity {\n" +
         "\t/* @Field(store = Store.YES, analyze = Analyze.YES, analyzer = @Analyzer(definition = \"stemmer\")) */\n" +
         "\toptional string f1 = 1;\n" +
         "\t/* @Field(store = Store.YES, analyze = Analyze.NO, indexNullAs = \"-1\") */\n" +
         "\toptional int32 f2 = 2;\n" +
         "}\n";

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
      ConfigurationBuilder cfg = getConfigurationBuilder();
      createClusteredCaches(getNodesCount(), cfg, true);

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
      metadataCache.put("sample_bank_account/bank.proto", Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader()));
      metadataCache.put("not_indexed.proto", NOT_INDEXED_PROTO_SCHEMA);
      metadataCache.put("custom_analyzer.proto", CUSTOM_ANALYZER_PROTO_SCHEMA);
      checkSchemaErrors(metadataCache);

      //initialize client-side serialization context
      SerializationContext serCtx = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      MarshallerRegistration.registerMarshallers(serCtx);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("not_indexed.proto", NOT_INDEXED_PROTO_SCHEMA));
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("custom_analyzer.proto", CUSTOM_ANALYZER_PROTO_SCHEMA));
      serCtx.registerMarshaller(new NotIndexedMarshaller());
      serCtx.registerMarshaller(new AnalyzerTestEntityMarshaller());
   }

   /**
    * Logs the Protobuf schema errors (if any) and fails the test appropriately.
    */
   protected void checkSchemaErrors(RemoteCache<String, String> metadataCache) {
      if (metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX)) {
         // The existence of this key indicates there are errors in some files
         String files = metadataCache.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
         for (String fname : files.split("\n")) {
            String errorKey = fname + ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX;
            log.errorf("Found errors in Protobuf schema file: %s\n%s\n", fname, metadataCache.get(errorKey));
         }

         fail("There are errors in the following Protobuf schema files:\n" + files);
      }
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
      Query q = createQueryFromString("from " + getModelFactory().getUserTypeName() + " u where u.creationDate = " + Instant.parse("2011-12-03T10:15:30Z").toEpochMilli());

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   /**
    * This test is overridden because instants need special handling for protobuf (being actually emulated as long
    * timestamps).
    */
   @Override
   public void testInstant2() {
      Query q = createQueryFromString("from " + getModelFactory().getUserTypeName() + " u where u.passwordExpirationDate = " + Instant.parse("2011-12-03T10:15:30Z").toEpochMilli());

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testCustomFieldAnalyzer() {
      Query q = createQueryFromString("from sample_bank_account.AnalyzerTestEntity where f1:'test'");

      List<AnalyzerTestEntity> list = q.list();
      assertEquals(2, list.size());
   }
}

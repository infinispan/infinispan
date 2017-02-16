package org.infinispan.client.hotrod.query;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertFalse;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
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
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.QueryStringTest;
import org.infinispan.query.dsl.embedded.testdomain.ModelFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterClass;
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
      metadataCache.put("sample_bank_account/bank.proto", Util.read(Util.getResourceAsStream("/sample_bank_account/bank.proto", getClass().getClassLoader())));
      metadataCache.put("not_indexed.proto", NOT_INDEXED_PROTO_SCHEMA);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      SerializationContext serCtx = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      MarshallerRegistration.registerMarshallers(serCtx);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("not_indexed.proto", NOT_INDEXED_PROTO_SCHEMA));
      serCtx.registerMarshaller(new NotIndexedMarshaller());
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return builder;
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN014036: Prefix, wildcard or regexp queries cannot be fuzzy.*")
   @Override
   public void testFullTextWildcardFuzzyNotAllowed() throws Exception {
      super.testFullTextWildcardFuzzyNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028526: Invalid query.*")
   @Override
   public void testFullTextRegexpFuzzyNotAllowed() throws Exception {
      super.testFullTextRegexpFuzzyNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*property is analyzed.*")
   @Override
   public void testExactMatchOnAnalyzedFieldNotAllowed() throws Exception {
      // exception is wrapped in HotRodClientException
      super.testExactMatchOnAnalyzedFieldNotAllowed();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*unless the property is indexed and analyzed.*")
   @Override
   public void testFullTextTermOnNonAnalyzedFieldNotAllowed() throws Exception {
      // exception is wrapped in HotRodClientException
      super.testFullTextTermOnNonAnalyzedFieldNotAllowed();
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }
}

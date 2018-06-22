package org.infinispan.client.hotrod.query;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
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
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.ModelFactoryPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.NotIndexedMarshaller;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.AbstractQueryDslTest;
import org.infinispan.query.dsl.embedded.testdomain.ModelFactory;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.impl.ProgrammaticSearchMappingProviderImpl;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test the disabling of indexing for message types that are annotated {@code @Indexed(false)}. Non-indexed querying
 * should still work.
 *
 * @author anistor@redhat.com
 * @since 9.3
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryDisableIndexingTest")
public class RemoteQueryDisableIndexingTest extends AbstractQueryDslTest {

   private static final String NOT_INDEXED_PROTO_SCHEMA = "package sample_bank_account;\n" +
         "/** @Indexed(false) */\n" +
         "message NotIndexed {\n" +
         "\toptional string notIndexedField = 1;\n" +
         "}\n";

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;
   protected Cache<Object, Object> cache;

   protected final String notIndexedProtoSchemaFile;

   protected RemoteQueryDisableIndexingTest(String notIndexedProtoSchemaFile) {
      this.notIndexedProtoSchemaFile = notIndexedProtoSchemaFile;
   }

   public RemoteQueryDisableIndexingTest() {
      this(NOT_INDEXED_PROTO_SCHEMA);
   }

   @BeforeClass
   protected void populateCache() {
      getCacheForWrite().put("notIndexed1", new NotIndexed("testing 123"));
      getCacheForWrite().put("notIndexed2", new NotIndexed("xyz"));
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
      metadataCache.put("not_indexed.proto", notIndexedProtoSchemaFile);
      RemoteQueryTestUtils.checkSchemaErrors(metadataCache);

      //initialize client-side serialization context
      SerializationContext serCtx = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      MarshallerRegistration.registerMarshallers(serCtx);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("not_indexed.proto", notIndexedProtoSchemaFile));
      serCtx.registerMarshaller(new NotIndexedMarshaller());
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

   public void testEmptyIndexIsPresent() {
      SearchIntegrator searchIntegrator = org.infinispan.query.Search.getSearchManager(cache).unwrap(SearchIntegrator.class);

      // we have indexing for remote query!
      assertTrue(searchIntegrator.getIndexBindings().containsKey(ProtobufValueWrapper.INDEXING_TYPE));

      // we have an index for this cache
      String indexName = ProgrammaticSearchMappingProviderImpl.getIndexName(cache.getName());
      assertNotNull(searchIntegrator.getIndexManager(indexName));

      // index must be empty
      assertEquals(0, searchIntegrator.getStatistics().getNumberOfIndexedEntities(ProtobufValueWrapper.class.getName()));
   }

   public void testEqNonIndexedType() {
      Query q = getQueryFactory().create("from sample_bank_account.NotIndexed where notIndexedField = 'testing 123'");

      List<NotIndexed> list = q.list();
      assertEquals(1, list.size());
      assertEquals("testing 123", list.get(0).notIndexedField);
   }
}

package org.infinispan.client.hotrod.query;


import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.TestEntity;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test build-in analyzers
 *
 * @since 9.3.2
 */
@Test(groups = "functional", testName = "client.hotrod.query.BuiltInAnalyzersTest")
public class BuiltInAnalyzersTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      return TestCacheManagerFactory.createServerModeCacheManager(builder);
   }

   @BeforeClass
   protected void registerProtobufSchema() throws Exception {
      String protoFile = Util.getResourceAsString("/analyzers.proto", getClass().getClassLoader());
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("analyzers.proto", protoFile);
      RemoteQueryTestUtils.checkSchemaErrors(metadataCache);

      SerializationContext serCtx = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("analyzers.proto", protoFile));
      serCtx.registerMarshaller(new TestEntity.TestEntityMarshaller());
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort()).marshaller(new ProtoStreamMarshaller());
      return new InternalRemoteCacheManager(builder.build());
   }

   @Test
   public void testKeywordAnalyzer() {
      RemoteCache<Integer, TestEntity> remoteCache = remoteCacheManager.getCache();
      TestEntity child = new TestEntity("name", "name", "name",
            "name-with-dashes", "name", "name", null);

      TestEntity parent = new TestEntity("name", "name", "name",
            "name-with-dashes", "name", "name", child);

      remoteCache.put(1, parent);

      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);

      assertEquals(1, queryFactory.create("From TestEntity where name4:'name-with-dashes'").getResultSize());
      assertEquals(1, queryFactory.create("From TestEntity p where p.child.name4:'name-with-dashes'").getResultSize());
   }

   @Test
   public void testShippedAnalyzers() {
      RemoteCache<Integer, TestEntity> remoteCache = remoteCacheManager.getCache();
      TestEntity testEntity = new TestEntity("Sarah-Jane Lee", "John McDougall", "James Connor",
            "Oswald Lee", "Jason Hawkings", "Gyorgy Constantinides");
      remoteCache.put(1, testEntity);

      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);

      assertEquals(1, queryFactory.create("From TestEntity where name1:'jane'").getResultSize());
      assertEquals(1, queryFactory.create("From TestEntity where name2:'McDougall'").getResultSize());
      assertEquals(1, queryFactory.create("From TestEntity where name3:'Connor'").getResultSize());
      assertEquals(1, queryFactory.create("From TestEntity where name4:'Oswald Lee'").getResultSize());
      assertEquals(1, queryFactory.create("From TestEntity where name5:'hawk'").getResultSize());
      assertEquals(1, queryFactory.create("From TestEntity where name6:'constan'").getResultSize());

      assertEquals(0, queryFactory.create("From TestEntity where name1:'sara'").getResultSize());
      assertEquals(0, queryFactory.create("From TestEntity where name2:'John McDougal'").getResultSize());
      assertEquals(0, queryFactory.create("From TestEntity where name3:'James-Connor'").getResultSize());
      assertEquals(0, queryFactory.create("From TestEntity where name4:'Oswald lee'").getResultSize());
      assertEquals(0, queryFactory.create("From TestEntity where name5:'json'").getResultSize());
      assertEquals(0, queryFactory.create("From TestEntity where name6:'Georje'").getResultSize());
   }
}

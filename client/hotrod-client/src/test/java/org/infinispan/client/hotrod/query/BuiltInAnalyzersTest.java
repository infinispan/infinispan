package org.infinispan.client.hotrod.query;


import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.client.hotrod.query.testdomain.protobuf.TestEntity;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.util.Util;
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
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("TestEntity");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      Cache<String, String> metadataCache = manager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      String protoFile = Util.getResourceAsString("/analyzers.proto", getClass().getClassLoader());
      metadataCache.put("analyzers.proto", protoFile);
      RemoteQueryTestUtils.checkSchemaErrors(metadataCache);

      manager.defineConfiguration("test", builder.build());
      return manager;
   }

   @BeforeClass
   protected void registerProtobufSchema() throws Exception {
      String protoFile = Util.getResourceAsString("/analyzers.proto", getClass().getClassLoader());
      SerializationContext serCtx = MarshallerUtil.getSerializationContext(remoteCacheManager);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("analyzers.proto", protoFile));
      serCtx.registerMarshaller(new TestEntity.TestEntityMarshaller());
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      return new InternalRemoteCacheManager(builder.build());
   }

   @Test
   public void testKeywordAnalyzer() {
      RemoteCache<Integer, TestEntity> remoteCache = remoteCacheManager.getCache("test");
      TestEntity child = new TestEntity("name", "name", "name",
            "name-with-dashes", "name", "name", null);

      TestEntity parent = new TestEntity("name", "name", "name",
            "name-with-dashes", "name", "name", child);

      remoteCache.put(1, parent);

      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);

      assertEquals(1, queryFactory.create("From TestEntity where name4:'name-with-dashes'").execute().hitCount().orElse(-1));
      assertEquals(1, queryFactory.create("From TestEntity p where p.child.name4:'name-with-dashes'").execute().hitCount().orElse(-1));
   }

   @Test
   public void testShippedAnalyzers() {
      RemoteCache<Integer, TestEntity> remoteCache = remoteCacheManager.getCache("test");
      TestEntity testEntity = new TestEntity("Sarah-Jane Lee", "John McDougall", "James Connor",
            "Oswald Lee", "Jason Hawkings", "Gyorgy Constantinides");
      remoteCache.put(1, testEntity);

      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);

      assertEquals(1, queryFactory.create("From TestEntity where name1:'jane'").execute().hitCount().orElse(-1));
      assertEquals(1, queryFactory.create("From TestEntity where name2:'McDougall'").execute().hitCount().orElse(-1));
      assertEquals(1, queryFactory.create("From TestEntity where name3:'Connor'").execute().hitCount().orElse(-1));
      assertEquals(1, queryFactory.create("From TestEntity where name4:'Oswald Lee'").execute().hitCount().orElse(-1));
      assertEquals(1, queryFactory.create("From TestEntity where name5:'hawk'").execute().hitCount().orElse(-1));
      assertEquals(1, queryFactory.create("From TestEntity where name6:'constan'").execute().hitCount().orElse(-1));

      assertEquals(0, queryFactory.create("From TestEntity where name1:'sara'").execute().hitCount().orElse(-1));
      assertEquals(0, queryFactory.create("From TestEntity where name2:'John McDougal'").execute().hitCount().orElse(-1));
      assertEquals(0, queryFactory.create("From TestEntity where name3:'James-Connor'").execute().hitCount().orElse(-1));
      assertEquals(0, queryFactory.create("From TestEntity where name4:'Oswald lee'").execute().hitCount().orElse(-1));
      assertEquals(0, queryFactory.create("From TestEntity where name5:'json'").execute().hitCount().orElse(-1));
      assertEquals(0, queryFactory.create("From TestEntity where name6:'Georje'").execute().hitCount().orElse(-1));
   }
}

package org.infinispan.client.hotrod.query;


import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.checkSchemaErrors;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.client.hotrod.query.testdomain.protobuf.TestEntity;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchema;
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
      manager.defineConfiguration("test", builder.build());
      return manager;
   }

   @BeforeClass
   protected void registerProtobufSchema() throws Exception {
      SerializationContext serCtx = MarshallerUtil.getSerializationContext(remoteCacheManager);
      TestEntitySCI.INSTANCE.register(serCtx);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(builder.build());
      remoteCacheManager.administration().schemas().createOrUpdate(TestEntitySCI.INSTANCE);
      checkSchemaErrors(remoteCacheManager);

      return remoteCacheManager;
   }

   @Test
   public void testKeywordAnalyzer() {
      RemoteCache<Integer, TestEntity> remoteCache = remoteCacheManager.getCache("test");
      TestEntity child = new TestEntity("name", "name", "name",
            "name-with-dashes", "name", "name", null);

      TestEntity parent = new TestEntity("name", "name", "name",
            "name-with-dashes", "name", "name", child);

      remoteCache.put(1, parent);

      assertEquals(1, remoteCache.query("From TestEntity where name4:'name-with-dashes'").execute().count().value());
      assertEquals(1, remoteCache.query("From TestEntity p where p.child.name4:'name-with-dashes'").execute().count().value());
   }

   @Test
   public void testShippedAnalyzers() {
      RemoteCache<Integer, TestEntity> remoteCache = remoteCacheManager.getCache("test");
      TestEntity testEntity = new TestEntity("Sarah-Jane Lee", "John McDougall", "James Connor",
            "Oswald Lee", "Jason Hawkings", "Gyorgy Constantinides");
      remoteCache.put(1, testEntity);

      assertEquals(1, remoteCache.query("From TestEntity where name1:'jane'").execute().count().value());
      assertEquals(1, remoteCache.query("From TestEntity where name2:'McDougall'").execute().count().value());
      assertEquals(1, remoteCache.query("From TestEntity where name3:'Connor'").execute().count().value());
      assertEquals(1, remoteCache.query("From TestEntity where name4:'Oswald Lee'").execute().count().value());
      assertEquals(1, remoteCache.query("From TestEntity where name5:'hawk'").execute().count().value());
      assertEquals(1, remoteCache.query("From TestEntity where name6:'constan'").execute().count().value());

      assertEquals(0, remoteCache.query("From TestEntity where name1:'sara'").execute().count().value());
      assertEquals(0, remoteCache.query("From TestEntity where name2:'John McDougal'").execute().count().value());
      assertEquals(0, remoteCache.query("From TestEntity where name3:'James-Connor'").execute().count().value());
      assertEquals(0, remoteCache.query("From TestEntity where name4:'Oswald lee'").execute().count().value());
      assertEquals(0, remoteCache.query("From TestEntity where name5:'json'").execute().count().value());
      assertEquals(0, remoteCache.query("From TestEntity where name6:'Georje'").execute().count().value());
   }

   @ProtoSchema(
         includeClasses = TestEntity.class,
         schemaFileName = "test.client.BuiltInAnalyzersTest.proto",
         schemaFilePath = "org/infinispan/client/hotrod",
         service = false
   )
   public interface TestEntitySCI extends GeneratedSchema {
      GeneratedSchema INSTANCE = new TestEntitySCIImpl();
   }
}

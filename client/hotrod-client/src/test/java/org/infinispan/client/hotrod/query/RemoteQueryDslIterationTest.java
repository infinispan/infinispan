package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.ModelFactoryPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.QueryDslIterationTest;
import org.infinispan.query.dsl.embedded.testdomain.ModelFactory;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Test for orderBy, max results, start offset and projections.
 *
 * @author rvansa@redhat.com
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryDslIterationTest")
public class RemoteQueryDslIterationTest extends QueryDslIterationTest {

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;

   @Override
   protected QueryFactory getQueryFactory() {
      return Search.getQueryFactory(remoteCache);
   }

   @Override
   protected ModelFactory getModelFactory() {
      return ModelFactoryPB.INSTANCE;
   }

   @Override
   protected BasicCache<Object, Object> getCacheForQuery() {
      return remoteCache;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getConfigurationBuilder();
      createClusteredCaches(1, builder);

      hotRodServer = TestHelper.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.read(Util.getResourceAsStream("/sample_bank_account/bank.proto", getClass().getClassLoader())));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return builder;
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   // disable tests that do not make sense in remote case
   @Override
   public void testIteration1() {
   }

   @Override
   public void testIteration2() {
   }

   @Override
   public void testIteration3() {
   }

   @Override
   public void testIteration4() {
   }

   @Override
   public void testIteration5() {
   }
}

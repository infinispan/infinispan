package org.infinispan.client.hotrod.query;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.protostream.sampledomain.NotIndexed;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.Account;
import org.infinispan.protostream.sampledomain.bank.Transaction;
import org.infinispan.protostream.sampledomain.bank.User;
import org.infinispan.query.dsl.embedded.AbstractQueryTest;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test query for message types that are not annotated with {@code @Indexed}. Non-indexed querying must always work.
 *
 * @author anistor@redhat.com
 * @since 9.3
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryDisableIndexingTest")
public class RemoteQueryDisableIndexingTest extends AbstractQueryTest {

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;
   protected Cache<Object, Object> cache;

   @BeforeClass
   protected void populateCache() {
      getCacheForWrite().put("notIndexed1", new NotIndexed("testing 123"));
      getCacheForWrite().put("notIndexed2", new NotIndexed("xyz"));
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
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().clusteredDefault();
      globalBuilder.serialization().addContextInitializers(TestDomainSCI.INSTANCE);
      createClusteredCaches(getNodesCount(), globalBuilder, getConfigurationBuilder(), true);

      cache = manager(0).getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort())
            .addContextInitializers(TestDomainSCI.INSTANCE);
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

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      killServers(hotRodServer);
      hotRodServer = null;
   }

   public void testEmptyIndexIsPresent() {
      SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);

      // we have indexing for remote query!
      assertNotNull(searchMapping.indexedEntity(User.ENTITY_NAME));
      assertNotNull(searchMapping.indexedEntity(Account.ENTITY_NAME));
      assertNotNull(searchMapping.indexedEntity(Transaction.ENTITY_NAME));

      // we have some indexes for this cache
      assertEquals(3, searchMapping.allIndexedEntities().size());
   }

   public void testEqNonIndexedType() {
      Query<NotIndexed> q = getCacheForQuery().query("from sample_domain.NotIndexed where notIndexedField = 'testing 123'");

      List<NotIndexed> list = q.execute().list();
      assertEquals(1, list.size());
      assertEquals("testing 123", list.get(0).notIndexedField);
   }
}

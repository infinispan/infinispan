package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.sampledomain.Account;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.CompatibilityProtoStreamMarshaller;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.*;

/**
 * Tests compatibility between remote query and embedded mode.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.marshall.EmbeddedCompatTest", groups = "functional")
@CleanupAfterMethod
public class EmbeddedCompatTest extends SingleCacheManagerTest {

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, Account> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = hotRodCacheConfiguration();
      CompatibilityProtoStreamMarshaller compatModeMarshaller = new CompatibilityProtoStreamMarshaller();
      builder.compatibility().enable().marshaller(compatModeMarshaller);
      builder.indexing().enable();
      builder.dataContainer().keyEquivalence(AnyEquivalence.getInstance());  // TODO [anistor] hacks!
      cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      cache = cacheManager.getCache();

      compatModeMarshaller.setCacheManager(cacheManager); //todo [anistor] this works only in programmatic config mode, but not in xml config!

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      remoteCache = remoteCacheManager.getCache();

      //initialize client-side serialization context
      SerializationContext clientSerCtx = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      MarshallerRegistration.registerMarshallers(clientSerCtx);
      clientSerCtx.registerMarshaller(EmbeddedAccount.class, new EmbeddedAccountMarshaller());

      //initialize server-side serialization context
      MarshallerRegistration.registerMarshallers(ProtobufMetadataManager.getSerializationContext(cacheManager));
      ProtobufMetadataManager.getSerializationContext(cacheManager).registerMarshaller(EmbeddedAccount.class, new EmbeddedAccountMarshaller());

      return cacheManager;
   }

   @AfterTest
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   public void testPutAndGet() throws Exception {
      Account account = createAccount();
      remoteCache.put(1, account);

      // try to get the object through the local cache interface and check it's the same object we put
      assertEquals(1, cache.keySet().size());
      Object key = cache.keySet().iterator().next();
      Object localObject = cache.get(key);
      assertNotNull(localObject);
      assertTrue(localObject instanceof EmbeddedAccount);
      assertAccount((EmbeddedAccount) localObject);

      // get the object through the remote cache interface and check it's the same object we put
      Account fromRemoteCache = remoteCache.get(1);
      assertAccount(fromRemoteCache);
   }

   public void testRemoteQuery() throws Exception {
      Account account = createAccount();
      remoteCache.put(1, account);

      // get account back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(EmbeddedAccount.class)
            .having("description").like("%test%").toBuilder()
            .build();
      List<Account> list = query.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(EmbeddedAccount.class, list.get(0).getClass());
      assertAccount(list.get(0));
   }

   public void testEmbeddedQuery() throws Exception {
      Account account = createAccount();
      remoteCache.put(1, account);

      // get account back from local cache via query and check its attributes
      org.apache.lucene.search.Query query = org.infinispan.query.Search.getSearchManager(cache)
            .buildQueryBuilderForClass(EmbeddedAccount.class).get()
            .keyword().wildcard().onField("description").matching("*test*").createQuery();
      CacheQuery cacheQuery = org.infinispan.query.Search.getSearchManager(cache).getQuery(query);
      List<Object> list = cacheQuery.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(EmbeddedAccount.class, list.get(0).getClass());
      assertAccount((Account) list.get(0));
   }

   private Account createAccount() {
      Account account = new Account();
      account.setId(1);
      account.setDescription("test description");
      account.setCreationDate(new Date());
      return account;
   }

   private void assertAccount(Account account) {
      assertEquals(1, account.getId());
      assertEquals("test description", account.getDescription());
   }
}

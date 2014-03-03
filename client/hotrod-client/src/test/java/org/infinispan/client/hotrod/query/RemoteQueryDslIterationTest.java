package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.*;

/**
 * Test for orderBy, max results, start offset and projections.
 *
 * @author rvansa@redhat.com
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryDslIterationTest")
public class RemoteQueryDslIterationTest extends SingleCacheManagerTest {

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<String, Object> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getConfigurationBuilder();

      cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      cache = cacheManager.getCache();

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();

      //initialize server-side serialization context
      cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class).registerProtofile("/sample_bank_account/bank.protobin");

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));

      return cacheManager;
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.indexing().enable()
            .addProperty("default.directory_provider", getLuceneDirectoryProvider())
            .addProperty("lucene_version", "LUCENE_CURRENT");

      return builder;
   }

   protected String getLuceneDirectoryProvider() {
      return "ram";
   }

   @AfterTest
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   @BeforeMethod(alwaysRun = true)
   protected void populateCache() throws Exception {
      User user1 = new User();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("White");

      User user2 = new User();
      user2.setId(2);
      user2.setName("Jack");
      user2.setSurname("Black");

      User user3 = new User();
      user3.setId(3);
      user3.setName("John");
      user3.setSurname("Brown");

      User user4 = new User();
      user4.setId(4);
      user4.setName("Michael");
      user4.setSurname("Black");

      remoteCache.put("user_" + user1.getId(), user1);
      remoteCache.put("user_" + user2.getId(), user2);
      remoteCache.put("user_" + user3.getId(), user3);
      remoteCache.put("user_" + user4.getId(), user4);
   }

   public void testOrderByAsc() throws Exception {
      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.ASC).build();

      assertEquals(4, q.getResultSize());

      List<User> list = q.list();
      assertEquals(4, list.size());
      checkNameOrder(list, true);
   }

   public void testOrderByDesc() throws Exception {
      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query q = qf.from(User.class)
            .orderBy("surname", SortOrder.DESC).build();

      assertEquals(4, q.getResultSize());

      List<User> list = q.list();
      assertEquals(4, list.size());
      checkSurnameOrder(list, false);
   }

   public void testMaxResults() throws Exception {
      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.ASC).maxResults(2).build();

      assertEquals(4, q.getResultSize());

      List<User> list = q.list();
      assertEquals(2, list.size());
      checkNameOrder(list, true);
   }

   public void testStartOffset() throws Exception {
      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.ASC).startOffset(2).build();

      assertEquals(4, q.getResultSize());

      List<User> list = q.list();
      assertEquals(2, list.size());
      checkNameOrder(list, true);
   }

   public void testProjection() throws Exception {
      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query q = qf.from(User.class)
            .setProjection("id", "name").maxResults(3).build();

      assertEquals(4, q.getResultSize());

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      for (Object[] u : list) {
         assertNotNull(u[1]);
         assertTrue(u[0] instanceof Integer);
      }
   }

   private void checkNameOrder(List<User> list, boolean isAsc) {
      String prevName = null;
      for (User u : list) {
         assertNotNull(u.getName());
         if (prevName != null) {
            int comp = u.getName().compareTo(prevName);
            assertTrue(isAsc ? comp >= 0 : comp <= 0);
         }
         prevName = u.getName();
      }
   }

   private void checkSurnameOrder(List<User> list, boolean isAsc) {
      String prevSurname = null;
      for (User u : list) {
         assertNotNull(u.getSurname());
         if (prevSurname != null) {
            int comp = u.getSurname().compareTo(prevSurname);
            assertTrue(isAsc ? comp >= 0 : comp <= 0);
         }
         prevSurname = u.getSurname();
      }
   }
}

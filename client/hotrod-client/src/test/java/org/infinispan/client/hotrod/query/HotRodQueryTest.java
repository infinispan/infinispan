package org.infinispan.client.hotrod.query;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AddressPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test query via Hot Rod on a LOCAL cache.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.HotRodQueryTest", groups = {"functional", "smoke"})
public class HotRodQueryTest extends SingleCacheManagerTest {

   private static final String TEST_CACHE_NAME = "userCache";

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Integer, User> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      gcb.jmx().enabled(true)
         .domain(getClass().getSimpleName())
         .mBeanServerLookup(mBeanServerLookup);
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      gcb.serialization().addContextInitializer(TestDomainSCI.INSTANCE);
      configure(gcb);

      ConfigurationBuilder builder = getConfigurationBuilder();

      cacheManager = TestCacheManagerFactory.createCacheManager(gcb, new ConfigurationBuilder());
      cacheManager.defineConfiguration(TEST_CACHE_NAME, builder.build());
      cache = cacheManager.getCache(TEST_CACHE_NAME);

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort()).addContextInitializer(TestDomainSCI.INSTANCE);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);
      return cacheManager;
   }

   protected void configure(GlobalConfigurationBuilder builder) {
      // no-op
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
             .addIndexedEntity("sample_bank_account.User")
             .addProperty("directory.type", "local-heap");
      return builder;
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      killServers(hotRodServer);
      hotRodServer = null;
   }

   @BeforeClass(alwaysRun = true)
   protected void populateCache() {
      User user1 = new UserPB();
      user1.setId(1);
      user1.setName("Tom");
      user1.setSurname("Cat");
      user1.setGender(User.Gender.MALE);
      user1.setAccountIds(Collections.singleton(12));
      Address address1 = new AddressPB();
      address1.setStreet("Dark Alley");
      address1.setPostCode("1234");
      user1.setAddresses(Collections.singletonList(address1));
      remoteCache.put(1, user1);

      User user2 = new UserPB();
      user2.setId(2);
      user2.setName("Adrian");
      user2.setSurname("Nistor");
      user2.setGender(User.Gender.MALE);
      Address address2 = new AddressPB();
      address2.setStreet("Old Street");
      address2.setPostCode("XYZ");
      user2.setAddresses(Collections.singletonList(address2));
      remoteCache.put(2, user2);
   }

   @Override
   protected void clearContent() {
      //Don't clear, this is destroying the index
   }

   public void testAttributeQuery() {
      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      assertNotNull(fromCache);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> query = qf.create("FROM sample_bank_account.User u WHERE u.name = 'Tom'");
      List<User> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(UserPB.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   public void testEmbeddedAttributeQuery() {
      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> query = qf.create("FROM sample_bank_account.User u WHERE u.addresses.postCode = '1234'");
      List<User> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(UserPB.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028503: Property addresses can not be selected from type sample_bank_account.User since it is an embedded entity.")
   public void testInvalidEmbeddedAttributeQuery() {
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<Object[]> q = qf.create("SELECT addresses FROM sample_bank_account.User");

      q.execute();  // exception expected
   }

   public void testProjections() {
      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<Object[]> query = qf.create("SELECT name, surname FROM sample_bank_account.User WHERE name = 'Tom'");

      List<Object[]> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Object[].class, list.get(0).getClass());
      assertEquals("Tom", list.get(0)[0]);
      assertEquals("Cat", list.get(0)[1]);
   }

   private void assertUser1(User user) {
      assertNotNull(user);
      assertEquals(1, user.getId());
      assertEquals("Tom", user.getName());
      assertEquals("Cat", user.getSurname());
      assertEquals(User.Gender.MALE, user.getGender());
      assertNotNull(user.getAccountIds());
      assertEquals(1, user.getAccountIds().size());
      assertTrue(user.getAccountIds().contains(12));
      assertNotNull(user.getAddresses());
      assertEquals(1, user.getAddresses().size());
      assertEquals("Dark Alley", user.getAddresses().get(0).getStreet());
      assertEquals("1234", user.getAddresses().get(0).getPostCode());
   }
}

package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AddressPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests query over Hot Rod in a three node cluster.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.MultiHotRodServerQueryTest", groups = "functional")
public class MultiHotRodServerQueryTest extends MultiHotRodServersTest {

   protected RemoteCache<Integer, User> remoteCache0;
   protected RemoteCache<Integer, User> remoteCache1;

   protected boolean useTransactions() {
      return false;
   }

   @Override
   protected void modifyGlobalConfiguration(GlobalConfigurationBuilder builder) {
      super.modifyGlobalConfiguration(builder);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, useTransactions()));
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("sample_bank_account.User");

      createHotRodServers(3, builder);

      waitForClusterToForm();

      remoteCache0 = client(0).getCache();
      remoteCache1 = client(1).getCache();
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDomainSCI.INSTANCE;
   }

   @BeforeClass(alwaysRun = true)
   protected void populateCache() {
      User user1 = new UserPB();
      user1.setId(1);
      user1.setName("Tom");
      user1.setSurname("Cat");
      user1.setGender(User.Gender.MALE);
      user1.setAge(5);
      user1.setAccountIds(Collections.singleton(12));
      Address address1 = new AddressPB();
      address1.setStreet("Dark Alley");
      address1.setPostCode("1234");
      user1.setAddresses(Collections.singletonList(address1));
      remoteCache0.put(1, user1);

      assertNotNull(remoteCache0.get(1));
      assertNotNull(remoteCache1.get(1));

      User user2 = new UserPB();
      user2.setId(2);
      user2.setName("Adrian");
      user2.setSurname("Nistor");
      user2.setGender(User.Gender.MALE);
      user2.setAge(22);
      Address address2 = new AddressPB();
      address2.setStreet("Old Street");
      address2.setPostCode("XYZ");
      user2.setAddresses(Collections.singletonList(address2));
      remoteCache1.put(2, user2);

      assertNotNull(remoteCache0.get(2));
      assertNotNull(remoteCache1.get(2));

      // this value should be ignored gracefully
      client(0).getCache().put("dummy", "a primitive value cannot be queried");
   }

   public void testAttributeQuery() {
      // get user back from remote cache and check its attributes
      User fromCache = remoteCache0.get(1);
      assertNotNull(fromCache);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache1);
      Query<User> query = qf.create("FROM sample_bank_account.User WHERE name = 'Tom'");
      List<User> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(UserPB.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   public void testGroupByQuery() {
      // get user back from remote cache and check its attributes
      User fromCache = remoteCache0.get(1);
      assertNotNull(fromCache);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache0);
      Query<Object[]> query = qf.create("SELECT name, COUNT(age) FROM sample_bank_account.User WHERE age >= 5 GROUP BY name ORDER BY name ASC");
      List<Object[]> list = query.execute().list();
      assertNotNull(list);
      assertEquals(2, list.size());
      assertEquals(Object[].class, list.get(0).getClass());
      assertEquals(Object[].class, list.get(1).getClass());
      assertEquals("Adrian", list.get(0)[0]);
      assertEquals("Tom", list.get(1)[0]);
   }

   public void testEmbeddedAttributeQuery() {
      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache1);
      Query<User> query = qf.create("FROM sample_bank_account.User u WHERE u.addresses.postCode = '1234'");
      List<User> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(UserPB.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028503: Property addresses can not be selected from type sample_bank_account.User since it is an embedded entity.")
   public void testInvalidEmbeddedAttributeQuery() {
      QueryFactory qf = Search.getQueryFactory(remoteCache1);
      Query<Object[]> q = qf.create("SELECT addresses FROM sample_bank_account.User");
      q.execute();  // exception expected
   }

   public void testProjections() {
      // get user back from remote cache and check its attributes
      User fromCache = remoteCache0.get(1);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache1);
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

package org.infinispan.client.hotrod.query;

import static org.infinispan.query.dsl.Expression.count;
import static org.infinispan.query.dsl.Expression.property;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AddressPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.ProtobufMetadataManager;
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

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      builder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      createHotRodServers(3, builder);

      waitForClusterToForm();

      remoteCache0 = client(0).getCache();
      remoteCache1 = client(1).getCache();
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      return super.createHotRodClientConfigurationBuilder(serverPort)
            .marshaller(new ProtoStreamMarshaller());
   }

   @BeforeClass(alwaysRun = true)
   protected void populateCache() throws Exception {
      //initialize server-side serialization context
      ProtobufMetadataManager protobufMetadataManager = manager(0).getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
      protobufMetadataManager.registerProtofile("sample_bank_account/bank.proto", read("/sample_bank_account/bank.proto"));
      assertNull(protobufMetadataManager.getFileErrors("sample_bank_account/bank.proto"));
      assertNull(protobufMetadataManager.getFilesWithErrors());

      //initialize client-side serialization context
      for (RemoteCacheManager rcm : clients) {
         MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(rcm));
      }

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

   private String read(String classPathResource) throws IOException {
      return Util.read(getClass().getResourceAsStream(classPathResource));
   }

   public void testAttributeQuery() throws Exception {
      // get user back from remote cache and check its attributes
      User fromCache = remoteCache0.get(1);
      assertNotNull(fromCache);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache1);
      Query query = qf.from(UserPB.class)
            .having("name").eq("Tom")
            .build();
      List<User> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(UserPB.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   public void testGroupByQuery() throws Exception {
      // get user back from remote cache and check its attributes
      User fromCache = remoteCache0.get(1);
      assertNotNull(fromCache);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache0);
      Query query = qf.from(UserPB.class)
            .select(property("name"), count("age"))
            .having("age").gte(5)
            .groupBy("name")
            .orderBy("name")
            .build();
      List<Object[]> list = query.list();
      assertNotNull(list);
      assertEquals(2, list.size());
      assertEquals(Object[].class, list.get(0).getClass());
      assertEquals(Object[].class, list.get(1).getClass());
      assertEquals("Adrian", list.get(0)[0]);
      assertEquals("Tom", list.get(1)[0]);
   }

   public void testEmbeddedAttributeQuery() throws Exception {
      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache1);
      Query query = qf.from(UserPB.class)
            .having("addresses.postCode").eq("1234")
            .build();
      List<User> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(UserPB.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028503: Property addresses can not be selected from type sample_bank_account.User since it is an embedded entity.")
   public void testInvalidEmbeddedAttributeQuery() throws Exception {
      QueryFactory qf = Search.getQueryFactory(remoteCache1);

      Query q = qf.from(UserPB.class)
            .select("addresses").build();

      q.list();  // exception expected
   }

   public void testProjections() throws Exception {
      // get user back from remote cache and check its attributes
      User fromCache = remoteCache0.get(1);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache1);
      Query query = qf.from(UserPB.class)
            .select("name", "surname")
            .having("name").eq("Tom")
            .build();

      List<Object[]> list = query.list();
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

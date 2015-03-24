package org.infinispan.all.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.infinispan.all.remote.sample.AddressPB;
import org.infinispan.all.remote.sample.UserPB;
import org.infinispan.all.remote.sample.marshallers.MarshallerRegistration;
import org.infinispan.all.remote.sample.testdomain.Address;
import org.infinispan.all.remote.sample.testdomain.User;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 * Tests for remote queries over HotRod on a local cache using RAM directory -- using infinispan-remote uber-jar.
 *
 * @author Adrian Nistor
 * @author Martin Gencur
 * @author Tomas Sykora
 */

@RunWith(Arquillian.class)
public class RemoteAllRemoteQueryTest {

   protected final String cacheContainerName;
   protected final String cacheName;

   protected static RemoteCacheManager remoteCacheManager;
   protected static RemoteCache<Integer, User> remoteCache;

   public RemoteAllRemoteQueryTest() throws IOException {

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
            .host("127.0.0.1")
            .port(11222)
            .marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      this.cacheContainerName = "clustered";
      this.cacheName = "localtestcache";

      remoteCache = remoteCacheManager.getCache(cacheName);

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(
            ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", read("/sample_bank_account//bank.proto"));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   private String read(String resourcePath) throws IOException {
      return Util.read(getClass().getResourceAsStream(resourcePath));
   }

   @Test
   public void testAttributeQuery() throws Exception {
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      assertUser(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(UserPB.class)
            .having("name").eq("Tom").toBuilder()
            .build();
      List<User> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(UserPB.class, list.get(0).getClass());
      assertUser(list.get(0));
   }

   @Test
   public void testEmbeddedAttributeQuery() throws Exception {
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(UserPB.class)
            .having("addresses.postCode").eq("1234").toBuilder()
            .build();
      List<User> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(UserPB.class, list.get(0).getClass());
      assertUser(list.get(0));
   }

   @Test
   public void testProjections() throws Exception {
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      assertUser(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(UserPB.class)
            .setProjection("name", "surname")
            .having("name").eq("Tom").toBuilder()
            .build();
      List<Object[]> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Object[].class, list.get(0).getClass());
      assertEquals("Tom", list.get(0)[0]);
      assertEquals("Cat", list.get(0)[1]);
   }

   private User createUser1() {
      User user = new UserPB();
      user.setId(1);
      user.setName("Tom");
      user.setSurname("Cat");
      user.setGender(User.Gender.MALE);
      user.setAccountIds(Collections.singleton(12));
      Address address = new AddressPB();
      address.setStreet("Dark Alley");
      address.setPostCode("1234");
      user.setAddresses(Collections.singletonList(address));
      return user;
   }

   private User createUser2() {
      User user = new UserPB();
      user.setId(1);
      user.setName("Adrian");
      user.setSurname("Nistor");
      user.setGender(User.Gender.MALE);
      Address address = new AddressPB();
      address.setStreet("Old Street");
      address.setPostCode("XYZ");
      user.setAddresses(Collections.singletonList(address));
      return user;
   }

   private void assertUser(User user) {
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

   @AfterClass
   public static void tearDown() {
      if (remoteCache != null) {
         remoteCache.clear();
      }
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
   }


}


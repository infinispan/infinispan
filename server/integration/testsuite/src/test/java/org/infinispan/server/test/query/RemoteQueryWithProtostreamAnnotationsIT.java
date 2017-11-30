package org.infinispan.server.test.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoMessage;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.test.category.Queries;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for remote queries over HotRod using Protobuf annotations.
 *
 * @author Adrian Nistor
 */
@Category(Queries.class)
@RunWith(Arquillian.class)
public class RemoteQueryWithProtostreamAnnotationsIT {

   private static final String cacheName = "localtestcache";

   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, AnnotatedUser> remoteCache;
   private RemoteCacheManagerFactory rcmFactory;

   @InfinispanResource("remote-query-1")
   protected RemoteInfinispanServer server;

   @ProtoDoc("@Indexed")
   @ProtoMessage(name = "User")
   public static class AnnotatedUser {

      private int id;

      private String name;

      private AnnotatedAddress address;

      @ProtoDoc("@IndexedField(index = false, store = false)")
      @ProtoField(number = 1, required = true)
      public int getId() {
         return id;
      }

      public void setId(int id) {
         this.id = id;
      }

      @ProtoDoc("@IndexedField")
      @ProtoField(number = 2)
      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      @ProtoDoc("@IndexedField")
      @ProtoField(number = 3)
      public AnnotatedAddress getAddress() {
         return address;
      }

      public void setAddress(AnnotatedAddress address) {
         this.address = address;
      }

      @Override
      public String toString() {
         return "AnnotatedUser{id=" + id + ", name='" + name + '\'' + ", address=" + address + '}';
      }
   }

   @ProtoMessage(name = "Address")
   public static class AnnotatedAddress {

      private String street;

      private String postCode;

      @ProtoField(number = 10)
      public String getStreet() {
         return street;
      }

      public void setStreet(String street) {
         this.street = street;
      }

      @ProtoField(number = 20)
      public String getPostCode() {
         return postCode;
      }

      public void setPostCode(String postCode) {
         this.postCode = postCode;
      }

      @Override
      public String toString() {
         return "AnnotatedAddress{street='" + street + '\'' + ", postCode='" + postCode + '\'' + '}';
      }
   }

   @Before
   public void setUp() throws Exception {
      rcmFactory = new RemoteCacheManagerFactory();
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
            .host(server.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server.getHotrodEndpoint().getPort())
            .marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = rcmFactory.createManager(clientBuilder);
      remoteCache = remoteCacheManager.getCache(cacheName);

      //initialize client-side serialization context
      SerializationContext serializationContext = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String protoFile = protoSchemaBuilder.fileName("test.proto")
            .addClass(AnnotatedUser.class)
            .build(serializationContext);

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("test.proto", protoFile);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
   }

   @After
   public void tearDown() {
      if (remoteCache != null) {
         remoteCache.clear();
      }
      if (rcmFactory != null) {
         rcmFactory.stopManagers();
      }
      rcmFactory = null;
   }

   @Test
   public void testAttributeQuery() {
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user1 back from remote cache and check its attributes
      AnnotatedUser fromCache = remoteCache.get(1);
      assertUser1(fromCache);

      // get user1 back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(AnnotatedUser.class)
            .having("name").eq("Tom")
            .build();
      List<AnnotatedUser> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(AnnotatedUser.class, list.get(0).getClass());
      assertUser1(list.get(0));

      // get user2 back from remote cache via query and check its attributes
      query = qf.from(AnnotatedUser.class)
            .having("address.postCode").eq("Xyz")
            .build();
      list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(AnnotatedUser.class, list.get(0).getClass());
      assertUser2(list.get(0));
   }

   private AnnotatedUser createUser1() {
      AnnotatedUser user = new AnnotatedUser();
      user.setId(1);
      user.setName("Tom");
      return user;
   }

   private AnnotatedUser createUser2() {
      AnnotatedAddress address = new AnnotatedAddress();
      address.setStreet("North street");
      address.setPostCode("Xyz");

      AnnotatedUser user = new AnnotatedUser();
      user.setId(2);
      user.setName("Adrian");
      user.setAddress(address);
      return user;
   }

   private void assertUser1(AnnotatedUser user) {
      assertNotNull(user);
      assertEquals(1, user.getId());
      assertEquals("Tom", user.getName());
   }

   private void assertUser2(AnnotatedUser user) {
      assertNotNull(user);
      assertEquals(2, user.getId());
      assertEquals("Adrian", user.getName());
      assertNotNull(user.getAddress());
      assertEquals("North street", user.getAddress().getStreet());
      assertEquals("Xyz", user.getAddress().getPostCode());
   }
}

package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
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

import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for remote queries over HotRod on a local cache using RAM directory.
 *
 * @author Adrian Nistor
 */
@Category({Queries.class})
@RunWith(Arquillian.class)
public class RemoteQueryWithProtostreamAnnotationsIT {

   private final String cacheName = "localtestcache";

   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, AnnotatedUser> remoteCache;
   private RemoteCacheManagerFactory rcmFactory;

   @InfinispanResource("remote-query")
   protected RemoteInfinispanServer server;

   @ProtoMessage(name = "User")
   public static class AnnotatedUser {

      private int id;

      private String name;

      @ProtoField(number = 1, required = true)
      public int getId() {
         return id;
      }

      public void setId(int id) {
         this.id = id;
      }

      @ProtoField(number = 2)
      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      @Override
      public String toString() {
         return "AnnotatedUser{id=" + id + ", name='" + name + "'}";
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
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      metadataCache.put("test.proto", protoFile);
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
   public void testAttributeQuery() throws Exception {
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user back from remote cache and check its attributes
      AnnotatedUser fromCache = remoteCache.get(1);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(AnnotatedUser.class)
            .having("name").eq("Tom").toBuilder()
            .build();
      List<AnnotatedUser> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(AnnotatedUser.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   private AnnotatedUser createUser1() {
      AnnotatedUser user = new AnnotatedUser();
      user.setId(1);
      user.setName("Tom");
      return user;
   }

   private AnnotatedUser createUser2() {
      AnnotatedUser user = new AnnotatedUser();
      user.setId(1);
      user.setName("Adrian");
      return user;
   }

   private void assertUser1(AnnotatedUser user) {
      assertNotNull(user);
      assertEquals(1, user.getId());
      assertEquals("Tom", user.getName());
   }
}

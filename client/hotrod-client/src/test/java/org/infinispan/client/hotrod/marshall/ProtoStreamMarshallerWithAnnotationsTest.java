package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoMessage;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.*;

/**
 * Tests integration between HotRod client and ProtoStream marshalling library.
 *
 * @author anistor@redhat.com
 * @since 7.1
 */
@Test(testName = "client.hotrod.marshall.ProtoStreamMarshallerWithAnnotationsTest", groups = "functional")
@CleanupAfterMethod
public class ProtoStreamMarshallerWithAnnotationsTest extends SingleCacheManagerTest {

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, AnnotatedUser> remoteCache;

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

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      remoteCache = remoteCacheManager.getCache();

      //initialize client-side serialization context
      SerializationContext serializationContext = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      protoSchemaBuilder.fileName("test.proto")
            .addClass(AnnotatedUser.class)
            .build(serializationContext);

      return cacheManager;
   }

   @AfterTest
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   public void testPutAndGet() throws Exception {
      AnnotatedUser user = createUser();
      remoteCache.put(1, user);

      // try to get the object through the local cache interface and check it's the same object we put
      assertEquals(1, cache.keySet().size());
      byte[] key = (byte[]) cache.keySet().iterator().next();
      Object localObject = cache.get(key);
      assertNotNull(localObject);
      assertTrue(localObject instanceof byte[]);
      Object unmarshalledObject = ProtobufUtil.fromWrappedByteArray(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager), (byte[]) localObject);
      assertTrue(unmarshalledObject instanceof AnnotatedUser);
      assertUser((AnnotatedUser) unmarshalledObject);

      // get the object through the remote cache interface and check it's the same object we put
      AnnotatedUser fromRemoteCache = remoteCache.get(1);
      assertUser(fromRemoteCache);
   }

   private AnnotatedUser createUser() {
      AnnotatedUser user = new AnnotatedUser();
      user.setId(33);
      user.setName("Tom");
      return user;
   }

   private void assertUser(AnnotatedUser user) {
      assertNotNull(user);
      assertEquals(33, user.getId());
      assertEquals("Tom", user.getName());
   }
}

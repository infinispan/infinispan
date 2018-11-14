package org.infinispan.server.test.query;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
public class RemoteQueryNearCacheIT {

   private static final String CONTAINER_NAME = "remote-query-minimal";

   @InfinispanResource(CONTAINER_NAME)
   protected RemoteInfinispanServer server;

   private RemoteCache<Integer, User> remoteCache;


   @Before
   public void setUp() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("127.0.0.1")
            .marshaller(new ProtoStreamMarshaller())
            .nearCache().mode(NearCacheMode.INVALIDATED).cacheNamePattern("indexed").maxEntries(10);

      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(builder.build());

      remoteCache = remoteCacheManager.getCache("indexed");

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader()));

      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   @After
   public void tearDown() {
      if (remoteCache != null) {
         remoteCache.clear();
         remoteCache.getRemoteCacheManager().stop();
      }
   }

   @Test
   @WithRunningServer(@RunningServer(name = CONTAINER_NAME))
   public void testReplaceValue() {
      User user = new User();
      user.setId(1);
      user.setName("John");
      user.setSurname("Doe");
      user.setGender(User.Gender.MALE);

      remoteCache.put(1, user);
      remoteCache.replace(1, user);

      assertNotNull(remoteCache.get(1));
   }
}

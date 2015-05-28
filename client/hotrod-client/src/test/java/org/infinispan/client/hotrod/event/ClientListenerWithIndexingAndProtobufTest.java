package org.infinispan.client.hotrod.event;


import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "functional", testName = "client.hotrod.event.ClientListenerWithIndexingAndProtobufTest")
public class ClientListenerWithIndexingAndProtobufTest extends MultiHotRodServersTest {

   private final int NUM_NODES = 2;

   private RemoteCache<Object, Object> remoteCache;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfgBuilder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      createHotRodServers(NUM_NODES, cfgBuilder);

      waitForClusterToForm();

      remoteCache = client(0).getCache();

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = client(0).getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.read(Util.getResourceAsStream("/sample_bank_account/bank.proto", getClass().getClassLoader())));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(client(0)));
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      return super.createHotRodClientConfigurationBuilder(serverPort)
            .marshaller(new ProtoStreamMarshaller());
   }

   public void testEventFilter() throws Exception {
      User user1 = new UserPB();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAge(22);

      NoopEventListener listener = new NoopEventListener();
      remoteCache.addClientListener(listener);

      assertEquals(0, listener.created);

      remoteCache.put("user_" + user1.getId(), user1);

      assertEquals(1, remoteCache.size());
      assertEquals(1, listener.created);

      remoteCache.removeClientListener(listener);
   }

   @ClientListener
   public static class NoopEventListener {

      int created = 0;

      @ClientCacheEntryCreated
      public void handleCreatedEvent(ClientCacheEntryCreatedEvent<?> e) {
         created++;
      }
   }
}

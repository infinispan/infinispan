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
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.filter.NamedFactory;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 * A simple remote listener test with filter and protobuf marshalling. This test uses raw key/value in events.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "functional", testName = "client.hotrod.event.ClientListenerWithFilterAndRawProtobufTest")
public class ClientListenerWithFilterAndRawProtobufTest extends MultiHotRodServersTest {

   private final int NUM_NODES = 2;

   private RemoteCache<Object, Object> remoteCache;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      createHotRodServers(NUM_NODES, cfgBuilder);
      waitForClusterToForm();

      for (int i = 0; i < NUM_NODES; i++) {
         server(i).addCacheEventFilterFactory("custom-filter-factory", new CustomCacheEventFilterFactory());
      }

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = client(0).getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.read(Util.getResourceAsStream("/sample_bank_account/bank.proto", getClass().getClassLoader())));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(client(0)));

      remoteCache = client(0).getCache();
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      return super.createHotRodClientConfigurationBuilder(serverPort)
            .marshaller(new ProtoStreamMarshaller());
   }

   public void testEventFilter() throws Exception {
      Object[] filterFactoryParams = new Object[]{"string_key_1", "user_1"};
      ClientEntryListener listener = new ClientEntryListener();
      remoteCache.addClientListener(listener, filterFactoryParams, null);

      User user1 = new UserPB();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAge(22);

      remoteCache.put("string_key_1", "string value 1");
      remoteCache.put("string_key_2", "string value 2");
      remoteCache.put("user_1", user1);

      assertEquals(3, remoteCache.keySet().size());
      assertEquals(2, listener.createEvents.size());
      assertEquals("string_key_1", listener.createEvents.get(0).getKey());
      assertEquals("user_1", listener.createEvents.get(1).getKey());
   }

   @ClientListener(filterFactoryName = "custom-filter-factory", useRawData = true)
   public static class ClientEntryListener {

      public List<ClientCacheEntryCreatedEvent> createEvents = new ArrayList<>();

      @ClientCacheEntryCreated
      @SuppressWarnings("unused")
      public void handleClientCacheEntryCreatedEvent(ClientCacheEntryCreatedEvent event) {
         createEvents.add(event);
      }
   }

   @NamedFactory(name = "custom-filter-factory")
   public static class CustomCacheEventFilterFactory implements CacheEventFilterFactory {

      private final ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();

      @Override
      public CacheEventFilter<byte[], byte[]> getFilter(Object[] params) {
         String firstParam;
         String secondParam;
         try {
            firstParam = (String) marshaller.objectFromByteBuffer((byte[]) params[0]);
            secondParam = (String) marshaller.objectFromByteBuffer((byte[]) params[1]);
         } catch (IOException e) {
            throw new RuntimeException(e);
         } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
         }
         return new CustomEventFilter(firstParam, secondParam);
      }
   }

   public static class CustomEventFilter implements CacheEventFilter<byte[], byte[]>, Serializable {

      private transient ProtoStreamMarshaller marshaller;
      private String firstParam;
      private String secondParam;

      public CustomEventFilter(String firstParam, String secondParam) {
         this.firstParam = firstParam;
         this.secondParam = secondParam;
      }

      private ProtoStreamMarshaller getMarshaller() {
         if (marshaller == null) {
            marshaller = new ProtoStreamMarshaller();
         }
         return marshaller;
      }

      @Override
      public boolean accept(byte[] key, byte[] oldValue, Metadata oldMetadata, byte[] newValue, Metadata newMetadata, EventType eventType) {
         try {
            String stringKey = (String) getMarshaller().objectFromByteBuffer(key);
            // this filter accepts only the two keys it received as params
            return firstParam.equals(stringKey) || secondParam.equals(stringKey);
         } catch (IOException e) {
            throw new RuntimeException(e);
         } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
         }
      }
   }
}

package org.infinispan.client.hotrod.event;


import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AddressPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.filter.JPACacheEventFilterConverterFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "functional", testName = "client.hotrod.event.RemoteListenerWithDslFilterTest")
public class RemoteListenerWithDslFilterTest extends MultiHotRodServersTest {

   private final int NUM_NODES = 5;

   private RemoteCache<Object, Object> remoteCache;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfgBuilder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      createHotRodServers(NUM_NODES, cfgBuilder);

      waitForClusterToForm();

      // Register the filter/converter factory. This should normally be discovered by the server via class path instead
      // of being added manually here, but this is ok in a test.
      JPACacheEventFilterConverterFactory factory = new JPACacheEventFilterConverterFactory();
      server(0).addCacheEventFilterConverterFactory(JPACacheEventFilterConverterFactory.FACTORY_NAME, factory);

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
      user1.setAccountIds(new HashSet<Integer>(Arrays.asList(1, 2)));
      user1.setNotes("Lorem ipsum dolor sit amet");

      Address address1 = new AddressPB();
      address1.setStreet("Main Street");
      address1.setPostCode("X1234");
      user1.setAddresses(Collections.singletonList(address1));

      User user2 = new UserPB();
      user2.setId(2);
      user2.setName("Spider");
      user2.setSurname("Man");
      user2.setGender(User.Gender.MALE);
      user2.setAge(32);
      user2.setAccountIds(Collections.singleton(3));

      Address address2 = new AddressPB();
      address2.setStreet("Old Street");
      address2.setPostCode("Y12");
      Address address3 = new AddressPB();
      address3.setStreet("Bond Street");
      address3.setPostCode("ZZ");
      user2.setAddresses(Arrays.asList(address2, address3));

      User user3 = new UserPB();
      user3.setId(3);
      user3.setName("Spider");
      user3.setSurname("Woman");
      user3.setGender(User.Gender.FEMALE);
      user3.setAge(40);
      user3.setAccountIds(Collections.<Integer>emptySet());

      SerializationContext serCtx = ProtoStreamMarshaller.getSerializationContext(client(0));
      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query query = qf.from(UserPB.class)
            .having("age").lte(32)
            .toBuilder().select("age").build();

      ClientEntryListener listener = new ClientEntryListener(serCtx);
      ClientEvents.addClientQueryListener(remoteCache, listener, query);

      remoteCache.put("user_" + user1.getId(), user1);
      remoteCache.put("user_" + user2.getId(), user2);
      remoteCache.put("user_" + user3.getId(), user3);

      assertEquals(3, remoteCache.size());
      assertEquals(2, listener.createEvents.size());

      remoteCache.removeClientListener(listener);
   }

   @ClientListener(filterFactoryName = ClientEvents.QUERY_DSL_FILTER_FACTORY_NAME,
         converterFactoryName = ClientEvents.QUERY_DSL_FILTER_FACTORY_NAME,
         useRawData = true, includeCurrentState = true)
   public class ClientEntryListener {

      public final List<FilterResult> createEvents = new ArrayList<FilterResult>();

      private final SerializationContext serializationContext;

      public ClientEntryListener(SerializationContext serializationContext) {
         this.serializationContext = serializationContext;
      }

      @ClientCacheEntryCreated
      public void handleClientCacheEntryCreatedEvent(ClientCacheEntryCustomEvent event) throws IOException {
         FilterResult r = (FilterResult) ProtobufUtil.fromWrappedByteArray(serializationContext, (byte[]) event.getEventData());
         createEvents.add(r);

         log.debugf("handleClientCacheEntryCreatedEvent instance=%s projection=%s sortProjection=%s\n",
                    r.getInstance(),
                    r.getProjection() == null ? null : Arrays.asList(r.getProjection()),
                    r.getSortProjection() == null ? null : Arrays.asList(r.getSortProjection()));
      }

      @ClientCacheEntryModified
      public void handleClientCacheEntryModifiedEvent(ClientCacheEntryModifiedEvent event) {
         log.debugf("handleClientCacheEntryModifiedEvent %s\n", event.getKey());
      }

      @ClientCacheEntryRemoved
      public void handleClientCacheEntryRemovedEvent(ClientCacheEntryRemovedEvent event) {
         log.debugf("handleClientCacheEntryRemovedEvent %s\n", event.getKey());
      }
   }
}

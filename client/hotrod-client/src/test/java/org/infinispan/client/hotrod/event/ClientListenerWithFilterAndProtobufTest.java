package org.infinispan.client.hotrod.event;


import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.User;
import org.testng.annotations.Test;


/**
 * A simple remote listener test with filter and protobuf marshalling. This test uses unmarshalled key/value in events.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "functional", testName = "client.hotrod.event.ClientListenerWithFilterAndProtobufTest")
public class ClientListenerWithFilterAndProtobufTest extends MultiHotRodServersTest {

   private final int NUM_NODES = 2;

   private RemoteCache<String, User> remoteCache;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultClusteredCacheConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      defaultClusteredCacheConfig.encoding().key().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      defaultClusteredCacheConfig.encoding().value().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(defaultClusteredCacheConfig);
      createHotRodServers(NUM_NODES, cfgBuilder);
      waitForClusterToForm();

      for (int i = 0; i < NUM_NODES; i++) {
         server(i).addCacheEventFilterFactory("custom-filter-factory", new CustomCacheEventFilterFactory());
      }

      remoteCache = client(0).getCache();
   }

   @Override
   protected List<SerializationContextInitializer> contextInitializers() {
      return Arrays.asList(TestDomainSCI.INSTANCE, ClientEventSCI.INSTANCE);
   }

   public void testEventFilter() throws Exception {
      Object[] filterFactoryParams = new Object[]{"John Doe", "Jane Doe"};
      ClientEntryListener<String> listener = new ClientEntryListener<>();
      remoteCache.addClientListener(listener, filterFactoryParams, null);
      remoteCache.putAll(User.data());

      List<String> keys = new ArrayList<>();
      keys.add(listener.createEvents.poll(5, TimeUnit.SECONDS).getKey());
      keys.add(listener.createEvents.poll(5, TimeUnit.SECONDS).getKey());
      assertThat(keys).containsExactlyInAnyOrder("John Doe", "Jane Doe");

      ClientCacheEntryCreatedEvent<String> e = listener.createEvents.poll(5, TimeUnit.SECONDS);
      assertNull(e, "No more elements expected in queue!");
   }

   @ClientListener(filterFactoryName = "custom-filter-factory")
   public static class ClientEntryListener<K> {

      public final BlockingQueue<ClientCacheEntryCreatedEvent<K>> createEvents = new LinkedBlockingQueue<>();

      @ClientCacheEntryCreated
      @SuppressWarnings("unused")
      public void handleClientCacheEntryCreatedEvent(ClientCacheEntryCreatedEvent<K> event) {
         createEvents.add(event);
      }
   }

   @NamedFactory(name = "custom-filter-factory")
   public static class CustomCacheEventFilterFactory implements CacheEventFilterFactory {

      @Override
      public CacheEventFilter<String, Object> getFilter(Object[] params) {
         String firstParam = (String) params[0];
         String secondParam = (String) params[1];
         return new CustomEventFilter(firstParam, secondParam);
      }
   }

   public static class CustomEventFilter implements CacheEventFilter<String, Object> {

      @ProtoField(1)
      final String firstParam;

      @ProtoField(2)
      final String secondParam;

      @ProtoFactory
      CustomEventFilter(String firstParam, String secondParam) {
         this.firstParam = firstParam;
         this.secondParam = secondParam;
      }

      @Override
      public boolean accept(String key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
         // this filter accepts only the two keys it received as params
         return firstParam.equals(key) || secondParam.equals(key);
      }
   }
}

package org.infinispan.client.hotrod.event;


import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.filter.Filters;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.User;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.server.core.query.impl.GlobalContextInitializer;
import org.infinispan.server.core.query.impl.filter.IckleCacheEventFilterConverterFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;


/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@Test(groups = "functional", testName = "client.hotrod.event.ClientListenerWithDslFilterObjectStorageTest")
public class ClientListenerWithDslFilterObjectStorageTest extends MultiHotRodServersTest {

   private static final int NUM_NODES = 5;

   private RemoteCache<String, User> remoteCache;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = getConfigurationBuilder();
      createHotRodServers(NUM_NODES, cfgBuilder);

      waitForClusterToForm();

      // Register the filter/converter factory. This should normally be discovered by the server via class path instead
      // of being added manually here, but this is ok in a test.
      IckleCacheEventFilterConverterFactory factory = new IckleCacheEventFilterConverterFactory();
      for (int i = 0; i < NUM_NODES; i++) {
         server(i).addCacheEventFilterConverterFactory(IckleCacheEventFilterConverterFactory.FACTORY_NAME, factory);
      }
      remoteCache = client(0).getCache();
   }

   @Override
   protected List<SerializationContextInitializer> contextInitializers() {
      return Arrays.asList(GlobalContextInitializer.INSTANCE, TestDomainSCI.INSTANCE, ClientEventSCI.INSTANCE);
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfgBuilder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cfgBuilder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cfgBuilder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntities(User.class);
      return cfgBuilder;
   }

   public void testEventFilter() {
      remoteCache.clear();
      remoteCache.putAll(User.data());

      SerializationContext serCtx = MarshallerUtil.getSerializationContext(client(0));
      Query<Object[]> query = remoteCache.query("SELECT age FROM sample_domain.User WHERE age <= :ageParam");
      query.setParameter("ageParam", 32);

      ClientEntryListener listener = new ClientEntryListener(serCtx);
      ClientEvents.addClientQueryListener(remoteCache, listener, query);
      expectElementsInQueue(listener.createEvents, 3);

      // re-put
      remoteCache.put("John Doe", remoteCache.get("John Doe"));
      remoteCache.put("Spider Man", remoteCache.get("Spider Man"));
      User spiderWoman = remoteCache.get("Spider Woman");
      spiderWoman.setAge(40);
      remoteCache.put("Spider Woman", spiderWoman);

      expectElementsInQueue(listener.modifyEvents, 2);

      remoteCache.removeClientListener(listener);
   }

   public void testEventFilterChangingParameter() {
      remoteCache.clear();
      remoteCache.putAll(User.data());

      SerializationContext serCtx = MarshallerUtil.getSerializationContext(client(0));

      Query<Object[]> query = remoteCache.query("SELECT age FROM sample_domain.User WHERE age <= :ageParam");
      query.setParameter("ageParam", 32);

      ClientEntryListener listener = new ClientEntryListener(serCtx);
      ClientEvents.addClientQueryListener(remoteCache, listener, query);
      expectElementsInQueue(listener.createEvents, 3);

      remoteCache.removeClientListener(listener);

      query.setParameter("ageParam", 31);

      listener = new ClientEntryListener(serCtx);
      ClientEvents.addClientQueryListener(remoteCache, listener, query);
      expectElementsInQueue(listener.createEvents, 2);

      remoteCache.removeClientListener(listener);
   }

   /**
    * Using grouping and aggregation with event filters is not allowed.
    */
   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN028509:.*")
   public void testDisallowGroupingAndAggregation() {
      Query<Object[]> query = remoteCache.query("SELECT MAX(age) FROM sample_domain.User WHERE age >= 20");

      ClientEntryListener listener = new ClientEntryListener(MarshallerUtil.getSerializationContext(client(0)));
      ClientEvents.addClientQueryListener(remoteCache, listener, query);
   }

   /**
    * Using non-raw listeners should throw an exception.
    */
   @Test(expectedExceptions = IncorrectClientListenerException.class, expectedExceptionsMessageRegExp = "ISPN004058:.*")
   public void testRequireRawDataListener() {
      Query<User> query = remoteCache.query("FROM sample_domain.User WHERE age >= 20");

      @ClientListener(filterFactoryName = Filters.QUERY_DSL_FILTER_FACTORY_NAME,
            converterFactoryName = Filters.QUERY_DSL_FILTER_FACTORY_NAME,
            useRawData = false, includeCurrentState = true)
      class DummyListener {
         @ClientCacheEntryCreated
         public void handleClientCacheEntryCreatedEvent(ClientCacheEntryCustomEvent event) {
         }
      }

      ClientEvents.addClientQueryListener(remoteCache, new DummyListener(), query);
   }

   /**
    * Using non-raw listeners should throw an exception.
    */
   @Test(expectedExceptions = IncorrectClientListenerException.class, expectedExceptionsMessageRegExp = "ISPN004059:.*")
   public void testRequireQueryDslFilterFactoryNameForListener() {
      Query<User> query = remoteCache.query("FROM sample_domain.User WHERE age >= 20");

      @ClientListener(filterFactoryName = "some-filter-factory-name",
            converterFactoryName = "some-filter-factory-name",
            useRawData = true, includeCurrentState = true)
      class DummyListener {
         @ClientCacheEntryCreated
         public void handleClientCacheEntryCreatedEvent(ClientCacheEntryCustomEvent event) {
         }
      }

      ClientEvents.addClientQueryListener(remoteCache, new DummyListener(), query);
   }

   private void expectElementsInQueue(BlockingQueue<?> queue, int numElements) {
      for (int i = 0; i < numElements; i++) {
         try {
            Object e = queue.poll(5, TimeUnit.SECONDS);
            assertNotNull(e, "Queue was empty!");
         } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for condition", e);
         }
      }
      try {
         // no more elements expected here
         Object e = queue.poll(5, TimeUnit.SECONDS);
         assertNull(e, "No more elements expected in queue!");
      } catch (InterruptedException e) {
         throw new AssertionError("Interrupted while waiting for condition", e);
      }
   }

   @ClientListener(filterFactoryName = Filters.QUERY_DSL_FILTER_FACTORY_NAME,
         converterFactoryName = Filters.QUERY_DSL_FILTER_FACTORY_NAME,
         useRawData = true, includeCurrentState = true)
   private static class ClientEntryListener {

      private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

      public final BlockingQueue<FilterResult> createEvents = new LinkedBlockingQueue<>();

      public final BlockingQueue<FilterResult> modifyEvents = new LinkedBlockingQueue<>();

      private final SerializationContext serializationContext;

      public ClientEntryListener(SerializationContext serializationContext) {
         this.serializationContext = serializationContext;
      }

      @ClientCacheEntryCreated
      public void handleClientCacheEntryCreatedEvent(ClientCacheEntryCustomEvent event) throws IOException {
         byte[] eventData = (byte[]) event.getEventData();
         FilterResult r = ProtobufUtil.fromWrappedByteArray(serializationContext, eventData);
         createEvents.add(r);

         log.debugf("handleClientCacheEntryCreatedEvent instance=%s projection=%s sortProjection=%s\n",
               r.getInstance(),
               r.getProjection() == null ? null : Arrays.asList(r.getProjection()),
               r.getSortProjection() == null ? null : Arrays.asList(r.getSortProjection()));
      }

      @ClientCacheEntryModified
      public void handleClientCacheEntryModifiedEvent(ClientCacheEntryCustomEvent event) throws IOException {
         byte[] eventData = (byte[]) event.getEventData();
         FilterResult r = ProtobufUtil.fromWrappedByteArray(serializationContext, eventData);
         modifyEvents.add(r);

         log.debugf("handleClientCacheEntryModifiedEvent instance=%s projection=%s sortProjection=%s\n",
               r.getInstance(),
               r.getProjection() == null ? null : Arrays.asList(r.getProjection()),
               r.getSortProjection() == null ? null : Arrays.asList(r.getSortProjection()));

      }

      @ClientCacheEntryRemoved
      public void handleClientCacheEntryRemovedEvent(ClientCacheEntryRemovedEvent event) {
         log.debugf("handleClientCacheEntryRemovedEvent %s\n", event.getKey());
      }
   }
}

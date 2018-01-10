package org.infinispan.server.test.query;

import static org.infinispan.query.dsl.Expression.param;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientEvents;
import org.infinispan.client.hotrod.filter.Filters;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.server.test.category.Queries;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Basic test for query DSL based remote event filters.
 *
 * @author anistor@redhat.com
 * @since 8.1
 */
@Category(Queries.class)
@RunWith(Arquillian.class)
public class RemoteListenerWithDslFilterIT extends RemoteQueryBaseIT {

   @InfinispanResource("remote-query-1")
   protected RemoteInfinispanServer server;

   public RemoteListenerWithDslFilterIT() {
      super("clustered", "localtestcache");
   }

   @Override
   protected RemoteInfinispanServer getServer() {
      return server;
   }

   @Test
   public void testEventFilter() {
      User user1 = new User();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAge(22);
      user1.setAccountIds(new HashSet<>(Arrays.asList(1, 2)));
      user1.setNotes("Lorem ipsum dolor sit amet");

      Address address1 = new Address();
      address1.setStreet("Main Street");
      address1.setPostCode("X1234");
      user1.setAddresses(Collections.singletonList(address1));

      User user2 = new User();
      user2.setId(2);
      user2.setName("Spider");
      user2.setSurname("Man");
      user2.setGender(User.Gender.MALE);
      user2.setAge(32);
      user2.setAccountIds(Collections.singleton(3));

      Address address2 = new Address();
      address2.setStreet("Old Street");
      address2.setPostCode("Y12");
      Address address3 = new Address();
      address3.setStreet("Bond Street");
      address3.setPostCode("ZZ");
      user2.setAddresses(Arrays.asList(address2, address3));

      User user3 = new User();
      user3.setId(3);
      user3.setName("Spider");
      user3.setSurname("Woman");
      user3.setGender(User.Gender.FEMALE);
      user3.setAge(31);
      user3.setAccountIds(Collections.emptySet());

      remoteCache.put(user1.getId(), user1);
      remoteCache.put(user2.getId(), user2);
      remoteCache.put(user3.getId(), user3);
      assertEquals(3, remoteCache.size());

      SerializationContext serCtx = ProtoStreamMarshaller.getSerializationContext(remoteCache.getRemoteCacheManager());
      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query query = qf.from(User.class)
            .having("age").lte(param("ageParam"))
            .select("age")
            .build()
            .setParameter("ageParam", 32);

      ClientEntryListener listener = new ClientEntryListener(serCtx);
      ClientEvents.addClientQueryListener(remoteCache, listener, query);
      expectElementsInQueue(listener.createEvents, 3);

      user3.setAge(40);
      remoteCache.put(user1.getId(), user1);
      remoteCache.put(user2.getId(), user2);
      remoteCache.put(user3.getId(), user3);

      assertEquals(3, remoteCache.size());
      expectElementsInQueue(listener.modifyEvents, 2);

      remoteCache.removeClientListener(listener);
   }

   private void expectElementsInQueue(BlockingQueue<?> queue, int numElements) {
      for (int i = 0; i < numElements; i++) {
         try {
            Object e = queue.poll(5, TimeUnit.SECONDS);
            assertNotNull("Queue was empty!", e);
         } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for condition", e);
         }
      }
      try {
         // no more elements expected here
         Object e = queue.poll(5, TimeUnit.SECONDS);
         assertNull("No more elements expected in queue!", e);
      } catch (InterruptedException e) {
         throw new AssertionError("Interrupted while waiting for condition", e);
      }
   }

   @ClientListener(filterFactoryName = Filters.QUERY_DSL_FILTER_FACTORY_NAME,
         converterFactoryName = Filters.QUERY_DSL_FILTER_FACTORY_NAME,
         useRawData = true, includeCurrentState = true)
   public static class ClientEntryListener {

      private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

      public final BlockingQueue<FilterResult> createEvents = new LinkedBlockingQueue<>();

      public final BlockingQueue<FilterResult> modifyEvents = new LinkedBlockingQueue<>();

      private final SerializationContext serializationContext;

      public ClientEntryListener(SerializationContext serializationContext) {
         this.serializationContext = serializationContext;
      }

      @ClientCacheEntryCreated
      @SuppressWarnings("unused")
      public void handleClientCacheEntryCreatedEvent(ClientCacheEntryCustomEvent event) throws IOException {
         FilterResult r = ProtobufUtil.fromWrappedByteArray(serializationContext, (byte[]) event.getEventData());
         createEvents.add(r);

         log.debugf("handleClientCacheEntryCreatedEvent instance=%s projection=%s sortProjection=%s\n",
               r.getInstance(),
               r.getProjection() == null ? null : Arrays.asList(r.getProjection()),
               r.getSortProjection() == null ? null : Arrays.asList(r.getSortProjection()));
      }

      @ClientCacheEntryModified
      @SuppressWarnings("unused")
      public void handleClientCacheEntryModifiedEvent(ClientCacheEntryCustomEvent event) throws IOException {
         FilterResult r = ProtobufUtil.fromWrappedByteArray(serializationContext, (byte[]) event.getEventData());
         modifyEvents.add(r);

         log.debugf("handleClientCacheEntryModifiedEvent instance=%s projection=%s sortProjection=%s\n",
               r.getInstance(),
               r.getProjection() == null ? null : Arrays.asList(r.getProjection()),
               r.getSortProjection() == null ? null : Arrays.asList(r.getSortProjection()));

      }

      @ClientCacheEntryRemoved
      @SuppressWarnings("unused")
      public void handleClientCacheEntryRemovedEvent(ClientCacheEntryRemovedEvent event) {
         log.debugf("handleClientCacheEntryRemovedEvent %s\n", event.getKey());
      }
   }
}

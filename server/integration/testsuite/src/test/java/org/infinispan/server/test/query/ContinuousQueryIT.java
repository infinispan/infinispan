package org.infinispan.server.test.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.event.ClientEvents;
import org.infinispan.client.hotrod.event.ContinuousQueryListener;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.server.test.category.Queries;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * 
 * Basic tests for continuous query over HotRod.
 * 
 * @author vjuranek
 * @since 8.1
 */

@Category({ Queries.class })
@RunWith(Arquillian.class)
public class ContinuousQueryIT extends RemoteQueryBaseIT {

   @InfinispanResource("remote-query-1")
   protected RemoteInfinispanServer server;

   public ContinuousQueryIT() {
      super("clustered", "localtestcache");
   }

   protected ContinuousQueryIT(String cacheContainerName, String cacheName) {
      super(cacheContainerName, cacheName);
   }

   @Override
   protected RemoteInfinispanServer getServer() {
      return server;
   }

   @Test
   public void testContinuousQuery() throws Exception {
      remoteCache.put(1, createUser(1, 25));
      remoteCache.put(2, createUser(2, 25));
      remoteCache.put(3, createUser(3, 20));
      assertEquals(3, remoteCache.size());

      QueryFactory<Query> qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(User.class).having("name").eq("user1").and().having("age").gt(20).toBuilder().build();

      final BlockingQueue<Object> joined = new ArrayBlockingQueue<Object>(10);
      final BlockingQueue<Object> left = new ArrayBlockingQueue<Object>(10);
      ContinuousQueryListener listener = new ContinuousQueryListener() {
         @Override
         public void resultJoining(Object key, Object value) {
            joined.add(key);
         }

         @Override
         public void resultLeaving(Object key) {
            left.add(key);
         }
      };
      Object clientListener = ClientEvents.addContinuousQueryListener(remoteCache, listener, query);
      
      assertNotNull(clientListener);
      expectElementsInQueue(joined, 1);
      expectElementsInQueue(left, 0);

      User user4 = createUser(4, 30);
      user4.setName("user1");
      remoteCache.put(4, user4);
      expectElementsInQueue(joined, 1);
      expectElementsInQueue(left, 0);
      
      User user1 = remoteCache.get(1);
      user1.setAge(19);
      remoteCache.put(1, user1);
      expectElementsInQueue(joined, 0);
      expectElementsInQueue(left, 1);
      
      remoteCache.clear();
      expectElementsInQueue(joined, 0);
      expectElementsInQueue(left, 1);
      
      remoteCache.removeClientListener(clientListener);
      user1.setAge(25);
      remoteCache.put(1, user1);
      expectElementsInQueue(joined, 0);
      expectElementsInQueue(left, 0);
   }
   
   private User createUser(int id, int age) {
      User user = new User();
      user.setId(id);
      user.setName("user" + id);
      user.setAge(age);
      user.setSurname("Doesn't matter");
      user.setGender(User.Gender.MALE);
      return user;
   }
   
   private void expectElementsInQueue(BlockingQueue<Object> queue, int numElements) {
      for (int i = 0; i < numElements; i++) {
         try {
            Object e = queue.poll(5, TimeUnit.SECONDS);
            assertNotNull(e);
         } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for condition", e);
         }
      }
      try {
         // no more elements expected here
         Object e = queue.poll(5, TimeUnit.SECONDS);
         assertNull(e);
      } catch (InterruptedException e) {
         throw new AssertionError("Interrupted while waiting for condition", e);
      }
   }

}

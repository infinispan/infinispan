package org.infinispan.server.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.api.continuous.ContinuousQueryListener;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Parameterized.class)
public class HotRodCacheContinuousQueries {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final boolean indexed;

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      List<Object[]> data = new ArrayList<>();
      data.add(new Object[]{true});
      data.add(new Object[]{false});
      return data;
   }

   public HotRodCacheContinuousQueries(boolean indexed) {
      this.indexed = indexed;
   }

   @Test
   public void testQueries() {
      RemoteCache<Integer, User> remoteCache = ClusteredIT.createQueryableCache(SERVER_TEST, indexed);

      remoteCache.put(1, createUser(1, 25));
      remoteCache.put(2, createUser(2, 25));
      remoteCache.put(3, createUser(3, 20));
      assertEquals(3, remoteCache.size());

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(User.class).having("name").eq("user1").and().having("age").gt(20).build();

      final BlockingQueue<Integer> joined = new LinkedBlockingQueue<>();
      final BlockingQueue<Integer> updated = new LinkedBlockingQueue<>();
      final BlockingQueue<Integer> left = new LinkedBlockingQueue<>();

      ContinuousQueryListener<Integer, User> listener = new ContinuousQueryListener<Integer, User>() {
         @Override
         public void resultJoining(Integer key, User value) {
            joined.add(key);
         }

         @Override
         public void resultUpdated(Integer key, User value) {
            updated.add(key);
         }

         @Override
         public void resultLeaving(Integer key) {
            left.add(key);
         }
      };
      ContinuousQuery<Integer, User> continuousQuery = Search.getContinuousQuery(remoteCache);
      continuousQuery.addContinuousQueryListener(query, listener);

      expectElementsInQueue(joined, 1);
      expectElementsInQueue(updated, 0);
      expectElementsInQueue(left, 0);

      User user4 = createUser(4, 30);
      user4.setName("user1");
      remoteCache.put(4, user4);
      expectElementsInQueue(joined, 1);
      expectElementsInQueue(updated, 0);
      expectElementsInQueue(left, 0);

      User user1 = remoteCache.get(1);
      user1.setAge(19);
      remoteCache.put(1, user1);
      expectElementsInQueue(joined, 0);
      expectElementsInQueue(updated, 0);
      expectElementsInQueue(left, 1);

      user4 = remoteCache.get(4);
      user4.setAge(32);
      remoteCache.put(4, user4);
      expectElementsInQueue(joined, 0);
      expectElementsInQueue(updated, 1);
      expectElementsInQueue(left, 0);

      remoteCache.clear();
      expectElementsInQueue(joined, 0);
      expectElementsInQueue(updated, 0);
      expectElementsInQueue(left, 1);

      continuousQuery.removeContinuousQueryListener(listener);
      user1.setAge(25);
      remoteCache.put(1, user1);
      expectElementsInQueue(joined, 0);
      expectElementsInQueue(updated, 0);
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
         Object e = queue.poll(500, TimeUnit.MILLISECONDS);
         assertNull("No more elements expected in queue!", e);
      } catch (InterruptedException e) {
         throw new AssertionError("Interrupted while waiting for condition", e);
      }
   }
}

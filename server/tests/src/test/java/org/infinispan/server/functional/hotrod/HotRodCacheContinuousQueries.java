package org.infinispan.server.functional.hotrod;

import static org.infinispan.server.functional.hotrod.HotRodCacheQueries.BANK_PROTO_FILE;
import static org.infinispan.server.functional.hotrod.HotRodCacheQueries.ENTITY_USER;
import static org.infinispan.server.test.core.Common.createQueryableCache;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HotRodCacheContinuousQueries {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testQueries(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, BANK_PROTO_FILE, ENTITY_USER);

      remoteCache.put(1, createUser(1, 25));
      remoteCache.put(2, createUser(2, 25));
      remoteCache.put(3, createUser(3, 20));
      assertEquals(3, remoteCache.size());

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> query = qf.create("FROM sample_bank_account.User WHERE name = 'user1' AND age > 20");

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
            assertNotNull(e, "Queue was empty!");
         } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for condition", e);
         }
      }
      try {
         // no more elements expected here
         Object e = queue.poll(500, TimeUnit.MILLISECONDS);
         assertNull(e, "No more elements expected in queue!");
      } catch (InterruptedException e) {
         throw new AssertionError("Interrupted while waiting for condition", e);
      }
   }
}

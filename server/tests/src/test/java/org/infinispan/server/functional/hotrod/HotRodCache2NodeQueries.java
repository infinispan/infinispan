package org.infinispan.server.functional.hotrod;

import static org.infinispan.server.test.core.Common.createQueryableCache;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.api.query.Query;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Holds the tests that only work when numNodes <= numOwners. See {@link HotRodCacheQueries} for tests
 * that should pass when numNodes > numOwners.
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HotRodCache2NodeQueries {

   public static final String ENTITY_USER = "sample_bank_account.User";

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   /**
    * This method doesn't work with numNodes > numNodes due to the fact that a distributed query needs to use
    * the segments as an in clause when a node doesn't own all segments thus lowering the maximum a user can query.
    */
   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testManyInClauses(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, TestDomainSCI.INSTANCE, ENTITY_USER);
      remoteCache.put(1, HotRodCacheQueries.createUser1());
      remoteCache.put(2, HotRodCacheQueries.createUser2());

      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      HotRodCacheQueries.assertUser1(fromCache);

      Set<String> values = new HashSet<>();
      values.add("Tom");
      for (int i = 0; i < 1024; i++) {
         values.add("test" + i);
      }
      Query<User> query = remoteCache.query("from sample_bank_account.User where name in (" + values.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")) +")");

      // this Ickle query translates to a BooleanQuery with 1025 clauses, 1 more than the max default (1024) so
      // executing it will fail unless the server jvm arg -Dinfinispan.query.lucene.max-boolean-clauses=1025 takes effect

      List<User> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(User.class, list.get(0).getClass());
      HotRodCacheQueries.assertUser1(list.get(0));
   }

   /**
    * This method doesn't work with numNodes > numNodes due to the fact that a distributed query needs to use
    * the segments as an in clause when a node doesn't own all segments thus lowering the maximum a user can query.
    */
   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testWayTooManyInClauses(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, TestDomainSCI.INSTANCE, ENTITY_USER);

      Set<String> values = new HashSet<>();
      // The clause count is slightly higher than 1025 as distributed queries use more clauses so
      // the value is increased
      for (int i = 0; i < 1026; i++) {
         values.add("test" + i);
      }

      Query<User> query = remoteCache.query("from sample_bank_account.User where name in (" + values.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(",")) +")");

      // this Ickle query translates to a BooleanQuery with 1026 clauses, 1 more than the configured
      // -Dinfinispan.query.lucene.max-boolean-clauses=1025, so executing the query is expected to fail

      if (indexed) {
         Exception expectedException = assertThrows(HotRodClientException.class, query::execute);
         assertTrue(expectedException.getMessage().contains("maxClauseCount is set to"));
      } else {
         query.execute();
      }
   }
}

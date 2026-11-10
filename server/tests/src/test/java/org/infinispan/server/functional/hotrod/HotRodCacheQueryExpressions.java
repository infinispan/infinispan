package org.infinispan.server.functional.hotrod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.functional.hotrod.HotRodCacheQueries.createUser1;
import static org.infinispan.server.functional.hotrod.HotRodCacheQueries.createUser2;
import static org.infinispan.server.test.core.Common.createQueryableCache;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.api.query.Query;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.User;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.jupiter.InfinispanServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 16.4
 **/
public class HotRodCacheQueryExpressions {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testUpdateStatementWithDivideFunction(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, TestDomainSCI.INSTANCE, User.ENTITY_NAME);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // divide() is the function form of the '/' operator; nested calls are the
      // only way to express repeated division, which the lexer cannot parse
      // with operators. (1 + 41) / 7 / 2 = 3.
      Query<User> query = remoteCache.query("UPDATE FROM sample_domain.User SET id = divide(divide(id + 41, 7), 2) WHERE id = 1");
      assertEquals(1, query.executeStatement());

      User updated = remoteCache.get(1);
      assertEquals(3, updated.getId());
      assertEquals("Tom", updated.getName());

      User unchanged = remoteCache.get(2);
      assertEquals(2, unchanged.getId());
   }

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testSelectFunctionCallConcat(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, TestDomainSCI.INSTANCE, User.ENTITY_NAME);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      Query<Object> query = remoteCache.query("SELECT concat(name, ' ', surname) FROM sample_domain.User WHERE name = 'Tom'");
      List<Object> list = query.execute().list();
      assertEquals(1, list.size());
      assertEquals("Tom Cat", ((Object[]) list.get(0))[0]);
   }

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testSelectFunctionCallUpper(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, TestDomainSCI.INSTANCE, User.ENTITY_NAME);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      Query<Object> query = remoteCache.query("SELECT upper(name) FROM sample_domain.User WHERE name = 'Adrian'");
      List<Object> list = query.execute().list();
      assertEquals(1, list.size());
      assertEquals("ADRIAN", ((Object[]) list.get(0))[0]);
   }

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testSelectNestedFunctionCall(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, TestDomainSCI.INSTANCE, User.ENTITY_NAME);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      Query<Object> query = remoteCache.query("SELECT upper(concat(name, ' ', surname)) FROM sample_domain.User WHERE name = 'Tom'");
      List<Object> list = query.execute().list();
      assertEquals(1, list.size());
      assertEquals("TOM CAT", ((Object[]) list.get(0))[0]);
   }

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testSelectFunctionCallMultipleProjections(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, TestDomainSCI.INSTANCE, User.ENTITY_NAME);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      Query<Object> query = remoteCache.query("SELECT upper(name), length(surname) FROM sample_domain.User WHERE name = 'Tom'");
      List<Object> list = query.execute().list();
      assertEquals(1, list.size());
      Object[] projections = (Object[]) list.get(0);
      assertEquals("TOM", projections[0]);
      assertEquals(3, projections[1]);
   }

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testSelectFunctionCallWithParameter(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, TestDomainSCI.INSTANCE, User.ENTITY_NAME);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      Query<Object> query = remoteCache.query("SELECT concat(name, ' - ', :label) FROM sample_domain.User WHERE name = 'Tom'");
      query.setParameter("label", "User");
      List<Object> list = query.execute().list();
      assertEquals(1, list.size());
      assertEquals("Tom - User", ((Object[]) list.get(0))[0]);
   }

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testSelectFunctionCallAllRows(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, TestDomainSCI.INSTANCE, User.ENTITY_NAME);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      Query<Object> query = remoteCache.query("SELECT concat(name, ' ', surname) FROM sample_domain.User");
      List<Object> list = query.execute().list();
      assertEquals(2, list.size());
      assertThat(list).extracting(r -> ((Object[]) r)[0]).containsExactlyInAnyOrder("Tom Cat", "Adrian Nistor");
   }

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testSelectFunctionCallUnknown(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, TestDomainSCI.INSTANCE, User.ENTITY_NAME);
      remoteCache.put(1, createUser1());

      Query<Object> query = remoteCache.query("SELECT nonExistentFunc(name) FROM sample_domain.User WHERE name = 'Tom'");
      assertThatThrownBy(query::execute).isInstanceOf(HotRodClientException.class);
   }
}

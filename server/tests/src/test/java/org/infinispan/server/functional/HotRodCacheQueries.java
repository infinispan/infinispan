package org.infinispan.server.functional;

import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Parameterized.class)
public class HotRodCacheQueries {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Rule
   public ExpectedException expectedException = ExpectedException.none();

   private final boolean indexed;

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      List<Object[]> data = new ArrayList<>();
      data.add(new Object[]{true});
      data.add(new Object[]{false});
      return data;
   }

   public HotRodCacheQueries(boolean indexed) {
      this.indexed = indexed;
   }

   @Test
   public void testAttributeQuery() {
      RemoteCache<Integer, User> remoteCache = ClusteredIT.createQueryableCache(SERVER_TEST, indexed);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> query = qf.create("FROM sample_bank_account.User WHERE name = 'Tom'");
      List<User> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(User.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   @Test
   public void testEmbeddedAttributeQuery() {
      RemoteCache<Integer, User> remoteCache = ClusteredIT.createQueryableCache(SERVER_TEST, indexed);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> query = qf.create("FROM sample_bank_account.User u WHERE u.addresses.postCode = '1234'");
      List<User> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(User.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   @Test
   public void testProjections() {
      RemoteCache<Integer, User> remoteCache = ClusteredIT.createQueryableCache(SERVER_TEST, indexed);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<Object[]> query = qf.create("SELECT name, surname FROM sample_bank_account.User WHERE name = 'Tom'");
      List<Object[]> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Object[].class, list.get(0).getClass());
      assertEquals("Tom", list.get(0)[0]);
      assertEquals("Cat", list.get(0)[1]);
   }

   /**
    * Sorting on a field that does not contain DocValues so Hibernate Search is forced to uninvert it.
    *
    * @see <a href="https://issues.jboss.org/browse/ISPN-5729">https://issues.jboss.org/browse/ISPN-5729</a>
    */
   @Test
   public void testUninverting() {
      RemoteCache<Integer, User> remoteCache = ClusteredIT.createQueryableCache(SERVER_TEST, indexed);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> query = qf.create("FROM sample_bank_account.User WHERE name = 'John' ORDER BY id ASC");
      assertEquals(0, query.execute().list().size());
   }

   @Test
   public void testIteratorWithQuery() {
      RemoteCache<Integer, User> remoteCache = ClusteredIT.createQueryableCache(SERVER_TEST, indexed);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> simpleQuery = qf.create("FROM sample_bank_account.User WHERE name = 'Tom'");

      List<Map.Entry<Object, Object>> entries = new ArrayList<>(1);
      try (CloseableIterator<Map.Entry<Object, Object>> iter = remoteCache.retrieveEntriesByQuery(simpleQuery, null, 3)) {
         while (iter.hasNext()) {
            entries.add(iter.next());
         }
      }
      assertEquals(1, entries.size());
      assertEquals("Cat", ((User) entries.get(0).getValue()).getSurname());
   }

   @Test
   public void testIteratorWithQueryAndProjections() {
      RemoteCache<Integer, User> remoteCache = ClusteredIT.createQueryableCache(SERVER_TEST, indexed);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<Object[]> simpleQuery = qf.create("SELECT surname, name FROM sample_bank_account.User WHERE name = 'Tom'");

      List<Map.Entry<Object, Object>> entries = new ArrayList<>(1);
      try (CloseableIterator<Map.Entry<Object, Object>> iter = remoteCache.retrieveEntriesByQuery(simpleQuery, null, 3)) {
         while (iter.hasNext()) {
            entries.add(iter.next());
         }
      }
      assertEquals(1, entries.size());
      Object[] projections = (Object[]) entries.get(0).getValue();
      assertEquals("Cat", projections[0]);
      assertEquals("Tom", projections[1]);
   }

   @Test
   public void testQueryViaRest() throws IOException {
      RemoteCache<Integer, User> remoteCache = ClusteredIT.createQueryableCache(SERVER_TEST, indexed);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      String query = "FROM sample_bank_account.User WHERE name='Adrian'";

      RestClient restClient = SERVER_TEST.newRestClient(new RestClientConfigurationBuilder());

      RestResponse response = sync(restClient.cache(SERVER_TEST.getMethodName()).query(query));

      Json results = Json.read(response.getBody());
      assertEquals(1, results.at("total_results").asInteger());
   }

   @Test
   public void testManyInClauses() {
      RemoteCache<Integer, User> remoteCache = ClusteredIT.createQueryableCache(SERVER_TEST, indexed);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      assertUser1(fromCache);

      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Set<String> values = new HashSet<>();
      values.add("Tom");
      for (int i = 0; i < 1024; i++) {
         values.add("test" + i);
      }
      Query<User> query = qf.from(User.class).having("name").in(values).build();

      // this Ickle query translates to a BooleanQuery with 1025 clauses, 1 more than the max default (1024) so
      // executing it will fail unless the server jvm arg -Dinfinispan.query.lucene.max-boolean-clauses=1025 takes effect

      List<User> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(User.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   @Test
   public void testWayTooManyInClauses() {
      RemoteCache<Integer, User> remoteCache = ClusteredIT.createQueryableCache(SERVER_TEST, indexed);

      if (indexed) {
         expectedException.expect(HotRodClientException.class);
         expectedException.expectMessage("org.apache.lucene.search.BooleanQuery$TooManyClauses: maxClauseCount is set to 1025");
      }

      Set<String> values = new HashSet<>();
      for (int i = 0; i < 1026; i++) {
         values.add("test" + i);
      }

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> query = qf.from(User.class).having("name").in(values).build();

      // this Ickle query translates to a BooleanQuery with 1026 clauses, 1 more than the configured
      // -Dinfinispan.query.lucene.max-boolean-clauses=1025, so executing the query is expected to fail

      query.execute();
   }

   private User createUser1() {
      User user = new User();
      user.setId(1);
      user.setName("Tom");
      user.setSurname("Cat");
      user.setGender(User.Gender.MALE);
      user.setAccountIds(Collections.singleton(12));
      Address address = new Address();
      address.setStreet("Dark Alley");
      address.setPostCode("1234");
      user.setAddresses(Collections.singletonList(address));
      return user;
   }

   private User createUser2() {
      User user = new User();
      user.setId(2);
      user.setName("Adrian");
      user.setSurname("Nistor");
      user.setGender(User.Gender.MALE);
      Address address = new Address();
      address.setStreet("Old Street");
      address.setPostCode("XYZ");
      user.setAddresses(Collections.singletonList(address));
      return user;
   }

   private void assertUser1(User user) {
      assertNotNull(user);
      assertEquals(1, user.getId());
      assertEquals("Tom", user.getName());
      assertEquals("Cat", user.getSurname());
      assertEquals(User.Gender.MALE, user.getGender());
      assertNotNull(user.getAccountIds());
      assertEquals(1, user.getAccountIds().size());
      assertTrue(user.getAccountIds().contains(12));
      assertNotNull(user.getAddresses());
      assertEquals(1, user.getAddresses().size());
      assertEquals("Dark Alley", user.getAddresses().get(0).getStreet());
      assertEquals("1234", user.getAddresses().get(0).getPostCode());
   }
}

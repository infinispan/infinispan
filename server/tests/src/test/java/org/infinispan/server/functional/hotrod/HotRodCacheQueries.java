package org.infinispan.server.functional.hotrod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.server.test.core.Common.createQueryableCache;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.functional.extensions.entities.Entities;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HotRodCacheQueries {

   public static final String BANK_PROTO_FILE = "/sample_bank_account/bank.proto";
   public static final String ENTITY_USER = "sample_bank_account.User";
   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testAttributeQuery(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, BANK_PROTO_FILE, ENTITY_USER);
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

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testEmbeddedAttributeQuery(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, BANK_PROTO_FILE, ENTITY_USER);
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

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testProjections(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, BANK_PROTO_FILE, ENTITY_USER);
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
   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testUninverting(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, BANK_PROTO_FILE, ENTITY_USER);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> query = qf.create("FROM sample_bank_account.User WHERE name = 'John' ORDER BY id ASC");
      assertEquals(0, query.execute().list().size());
   }

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testIteratorWithQuery(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, BANK_PROTO_FILE, ENTITY_USER);
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

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testIteratorWithQueryAndProjections(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, BANK_PROTO_FILE, ENTITY_USER);
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

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testQueryViaRest(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, BANK_PROTO_FILE, ENTITY_USER);
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      String query = "FROM sample_bank_account.User WHERE name='Adrian'";

      RestClient restClient = SERVERS.rest().withClientConfiguration(new RestClientConfigurationBuilder()).get();
      try (RestResponse response = sync(restClient.cache(SERVERS.getMethodName()).query(query))) {
         Json results = Json.read(response.getBody());
         if (Boolean.getBoolean(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_NEWER_THAN_14)) {
            assertEquals(1, results.at("hit_count").asInteger());
         } else {
            assertEquals(1, results.at("total_results").asInteger());
         }
      }
   }

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testManyInClauses(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, BANK_PROTO_FILE, ENTITY_USER);
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

   @ParameterizedTest
   @ValueSource(booleans = {true, false})
   public void testWayTooManyInClauses(boolean indexed) {
      RemoteCache<Integer, User> remoteCache = createQueryableCache(SERVERS, indexed, BANK_PROTO_FILE, ENTITY_USER);

      Set<String> values = new HashSet<>();
      for (int i = 0; i < 1026; i++) {
         values.add("test" + i);
      }

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> query = qf.from(User.class).having("name").in(values).build();

      // this Ickle query translates to a BooleanQuery with 1026 clauses, 1 more than the configured
      // -Dinfinispan.query.lucene.max-boolean-clauses=1025, so executing the query is expected to fail

      if (indexed) {
         Exception expectedException = assertThrows(HotRodClientException.class, query::execute);
         assertTrue(expectedException.getMessage().contains("maxClauseCount is set to 1025"));
      } else {
         query.execute();
      }
   }

   @Test
   public void testWithSCI() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addContextInitializer(Entities.INSTANCE);

      org.infinispan.configuration.cache.ConfigurationBuilder cache = new org.infinispan.configuration.cache.ConfigurationBuilder();
      cache.clustering().cacheMode(CacheMode.DIST_SYNC).encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);

      RemoteCache<String, Entities.Person> peopleCache = SERVERS.hotrod().withClientConfiguration(builder).withServerConfiguration(cache).create();
      RemoteCache<String, String> metadataCache = peopleCache.getRemoteCacheContainer().getCache(PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put(Entities.INSTANCE.getProtoFileName(), Entities.INSTANCE.getProtoFile());

      Map<String, Entities.Person> people = new HashMap<>();
      people.put("1", new Entities.Person("Oihana", "Rossignol", 2016, "Paris"));
      people.put("2", new Entities.Person("Elaia", "Rossignol", 2018, "Paris"));
      people.put("3", new Entities.Person("Yago", "Steiner", 2013, "Saint-Mandé"));
      people.put("4", new Entities.Person("Alberto", "Steiner", 2016, "Paris"));
      peopleCache.putAll(people);

      QueryFactory queryFactory = Search.getQueryFactory(peopleCache);
      Query<Entities.Person> query = queryFactory.create("FROM Person p where p.lastName = :lastName");
      query.setParameter("lastName", "Rossignol");
      List<Entities.Person> rossignols = query.execute().list();
      assertThat(rossignols).extracting("firstName").containsExactlyInAnyOrder("Oihana", "Elaia");

      RestClient restClient = SERVERS.rest().get();
      try (RestResponse response = sync(restClient.cache(peopleCache.getName()).entries(1000))) {
         if (response.getStatus() != 200) {
            fail(response.getBody());
         }

         Collection<?> entities = (Collection<?>) Json.read(response.getBody()).getValue();
         assertThat(entities).hasSize(4);
      }
   }

   public static User createUser1() {
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

   public static User createUser2() {
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

   public static void assertUser1(User user) {
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

package org.infinispan.client.hotrod.stress;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.marshall.EmbeddedUserMarshaller;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS;
import org.infinispan.query.remote.CompatibilityProtoStreamMarshaller;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Perf test for remote query. This test runs in compat mode so we can also run queries with lucene directly and compare
 * the performance.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "stress", testName = "client.hotrod.stress.RemoteQueryDslPerfTest")
public class RemoteQueryDslPerfTest extends MultipleCacheManagersTest {

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;
   protected Cache<Object, Object> cache;

   @Override
   protected void clearContent() {
      // Don't clear, this is destroying the index
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.compatibility().enable().marshaller(new CompatibilityProtoStreamMarshaller());
      builder.dataContainer().keyEquivalence(AnyEquivalence.getInstance());
      builder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(1, builder);

      cache = manager(0).getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();

      //initialize server-side serialization context
      ProtobufMetadataManager protobufMetadataManager = manager(0).getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
      protobufMetadataManager.registerProtofile("sample_bank_account/bank.proto", Util.read(Util.getResourceAsStream("/sample_bank_account/bank.proto", getClass().getClassLoader())));
      assertNull(protobufMetadataManager.getFileErrors("sample_bank_account/bank.proto"));
      assertNull(protobufMetadataManager.getFilesWithErrors());
      protobufMetadataManager.registerMarshaller(new EmbeddedUserMarshaller());

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   @BeforeClass(alwaysRun = true)
   protected void populateCache() throws Exception {
      final int loops = 10000;
      for (int i = 0; i < loops; i++) {
         // create the test objects
         User user1 = new UserHS();
         int id1 = i * 10 + 1;
         user1.setId(id1);
         user1.setName("John" + id1);
         user1.setSurname("Doe" + id1);
         user1.setAge(22);
         user1.setAccountIds(new HashSet<Integer>(Arrays.asList(1, 2)));
         user1.setNotes("Lorem ipsum dolor sit amet");

         User user2 = new UserHS();
         int id2 = i * 10 + 2;
         user2.setId(id2);
         user2.setName("Spider" + id2);
         user2.setSurname("Man" + id2);
         user2.setAccountIds(Collections.singleton(3));

         User user3 = new UserHS();
         int id3 = i * 10 + 3;
         user3.setId(id3);
         user3.setName("Spider" + id3);
         user3.setSurname("Woman" + id3);
         user3.setAccountIds(Collections.<Integer>emptySet());

         cache.put("user_" + user1.getId(), user1);
         cache.put("user_" + user2.getId(), user2);
         cache.put("user_" + user3.getId(), user3);
      }
   }

   public void testRemoteQueryDslExecution() throws Exception {
      QueryFactory qf = org.infinispan.client.hotrod.Search.getQueryFactory(remoteCache);
      QueryBuilder<Query> qb = qf.from("sample_bank_account.User")
            .having("name").eq("John1")
            .toBuilder();

      final int loops = 100000;
      final long startTs = System.nanoTime();
      for (int i = 0; i < loops; i++) {
         Query q = qb.build();
         List<User> list = q.list();
         assertEquals(1, list.size());
         assertEquals("John1", list.get(0).getName());
      }
      final long duration = (System.nanoTime() - startTs) / loops;

      // this is around 600 us
      System.out.printf("Remote execution took %d us per query\n", TimeUnit.NANOSECONDS.toMicros(duration));
   }

   public void testEmbeddedQueryDslExecution() throws Exception {
      QueryFactory qf = org.infinispan.query.Search.getQueryFactory(cache);
      QueryBuilder<Query> qb = qf.from(UserHS.class)
            .having("name").eq("John1")
            .toBuilder();

      final int loops = 100000;
      final long startTs = System.nanoTime();
      for (int i = 0; i < loops; i++) {
         Query q = qb.build();
         List<User> list = q.list();
         assertEquals(1, list.size());
         assertEquals("John1", list.get(0).getName());
      }
      final long duration = (System.nanoTime() - startTs) / loops;

      // this is around 300 us
      System.out.printf("Embedded execution took %d us per query\n", TimeUnit.NANOSECONDS.toMicros(duration));
   }

   public void testEmbeddedLuceneQueryExecution() throws Exception {
      SearchManager searchManager = org.infinispan.query.Search.getSearchManager(cache);
      org.apache.lucene.search.Query query = searchManager.buildQueryBuilderForClass(UserHS.class).get()
            .keyword().onField("name").matching("John1").createQuery();

      final int loops = 100000;
      final long startTs = System.nanoTime();
      for (int i = 0; i < loops; i++) {
         CacheQuery cacheQuery = searchManager.getQuery(query);
         List<Object> list = cacheQuery.list();
         assertEquals(1, list.size());
         assertEquals("John1", ((User) list.get(0)).getName());
      }
      final long duration = (System.nanoTime() - startTs) / loops;

      // this is around 300 us
      System.out.printf("Embedded HS execution took %d us per query\n", TimeUnit.NANOSECONDS.toMicros(duration));
   }
}

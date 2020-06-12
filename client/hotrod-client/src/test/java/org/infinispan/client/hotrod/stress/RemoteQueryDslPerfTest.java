package org.infinispan.client.hotrod.stress;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Perf test for remote query. This test runs storing objects so we can also run queries with lucene directly and
 * compare the performance.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "stress", testName = "client.hotrod.stress.RemoteQueryDslPerfTest", timeOut = 15 * 60 * 1000)
public class RemoteQueryDslPerfTest extends MultipleCacheManagersTest {

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;
   protected Cache<Object, Object> cache;

   private static final int WRITE_LOOPS = 10000;
   private static final int QUERY_LOOPS = 100000;

   @Override
   protected void clearContent() {
      // Don't clear, this is destroying the index
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.encoding().key().mediaType(APPLICATION_OBJECT_TYPE);
      builder.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);
      builder.indexing().enable()
             .addIndexedEntity(UserHS.class)
             .addProperty("default.directory_provider", "local-heap")
             .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(1, TestDomainSCI.INSTANCE, builder);

      cache = manager(0).getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort()).addContextInitializer(TestDomainSCI.INSTANCE);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   @BeforeClass(alwaysRun = true)
   protected void populateCache() {
      for (int i = 0; i < WRITE_LOOPS; i++) {
         // create the test objects
         User user1 = new UserHS();
         int id1 = i * 10 + 1;
         user1.setId(id1);
         user1.setName("John" + id1);
         user1.setSurname("Doe" + id1);
         user1.setAge(22);
         user1.setAccountIds(new HashSet<>(Arrays.asList(1, 2)));
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

         cache.put("user_" + user1.getId(), user1);
         cache.put("user_" + user2.getId(), user2);
         cache.put("user_" + user3.getId(), user3);
      }
   }

   public void testRemoteQueryDslExecution() {
      QueryFactory qf = org.infinispan.client.hotrod.Search.getQueryFactory(remoteCache);
      String queryString = "FROM sample_bank_account.User WHERE name = 'John1'";

      final long startTs = System.nanoTime();
      for (int i = 0; i < QUERY_LOOPS; i++) {
         Query<User> q = qf.create(queryString);
         List<User> list = q.execute().list();
         assertEquals(1, list.size());
         assertEquals("John1", list.get(0).getName());
      }
      final long duration = (System.nanoTime() - startTs) / QUERY_LOOPS;

      // this is around 600 us
      System.out.printf("Remote execution took %d us per query\n", TimeUnit.NANOSECONDS.toMicros(duration));
   }

   public void testEmbeddedQueryDslExecution() {
      QueryFactory qf = org.infinispan.query.Search.getQueryFactory(cache);
      String queryString = String.format("FROM %s WHERE name = 'John1'", UserHS.class.getName());

      final long startTs = System.nanoTime();
      for (int i = 0; i < QUERY_LOOPS; i++) {
         Query<User> q = qf.create(queryString);
         List<User> list = q.execute().list();
         assertEquals(1, list.size());
         assertEquals("John1", list.get(0).getName());
      }
      final long duration = (System.nanoTime() - startTs) / QUERY_LOOPS;

      // this is around 300 us
      System.out.printf("Embedded execution took %d us per query\n", TimeUnit.NANOSECONDS.toMicros(duration));
   }
}

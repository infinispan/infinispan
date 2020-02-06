package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createServerModeCacheManager;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.dsl.Query;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Tests for the MassIndexer when using storing protobuf
 */
@Test(testName = "client.hotrod.query.ReindexCacheTest", groups = "functional")
public class ReindexCacheTest extends SingleHotRodServerTest {

   private static final String USER_CACHE = "users";

   private static final int NUM_ENTRIES = 50;

   private StorageType storageType;

   @Factory
   public Object[] factory() {
      return new Object[]{
            new ReindexCacheTest().storageType(StorageType.OBJECT),
            new ReindexCacheTest().storageType(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = createServerModeCacheManager(contextInitializer(), hotRodCacheConfiguration());
      cacheManager.defineConfiguration(USER_CACHE, hotRodCacheConfiguration(buildIndexedConfig()).build());
      return cacheManager;
   }

   @Override
   protected String parameters() {
      return "storageType-" + storageType;
   }

   private ReindexCacheTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   public ConfigurationBuilder buildIndexedConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(new ConfigurationBuilder());
      builder.memory().storageType(storageType).indexing().enable()
            .addProperty("default.directory_provider", "local-heap");
      return builder;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDomainSCI.INSTANCE;
   }

   @Test
   public void testMassIndexing() {
      RemoteCache<Integer, UserPB> userCache = remoteCacheManager.getCache(USER_CACHE);
      getUsersPB().forEach(u -> userCache.put(u.getId(), u));

      assertEquals(query(userCache), NUM_ENTRIES);

      wipeIndexes();
      assertIndexEmpty();
      reindex();

      assertEquals(query(userCache), NUM_ENTRIES);
   }

   private void wipeIndexes() {
      Cache<?, ?> cache = cacheManager.getCache(USER_CACHE);
      MassIndexer massIndexer = org.infinispan.query.Search.getSearchManager(cache).getMassIndexer();
      CompletionStages.join(massIndexer.purge());
   }

   private void assertIndexEmpty() {
      assertEquals(query(remoteCacheManager.getCache(USER_CACHE)), 0);
   }

   private void reindex() {
      Cache<?, ?> cache = cacheManager.getCache(USER_CACHE);
      MassIndexer massIndexer = org.infinispan.query.Search.getSearchManager(cache).getMassIndexer();
      massIndexer.start();
   }

   private int query(RemoteCache<?, ?> cache) {
      Query q = Search.getQueryFactory(cache).from(UserPB.class).build();
      return q.list().size();
   }

   private List<UserPB> getUsersPB() {
      return IntStream.range(0, NUM_ENTRIES).boxed().map(i -> {
         UserPB userPB = new UserPB();
         userPB.setId(i);
         userPB.setName("name" + i);
         userPB.setSurname("surname" + i);
         return userPB;
      }).collect(Collectors.toList());
   }
}

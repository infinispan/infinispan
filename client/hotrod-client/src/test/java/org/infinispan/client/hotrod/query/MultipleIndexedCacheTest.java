package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AccountPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.dsl.Query;
import org.testng.annotations.Test;

/**
 * Tests for multiple indexed caches in the server.
 */
@Test(testName = "client.hotrod.query.MultipleIndexedCacheTest", groups = "functional")
public class MultipleIndexedCacheTest extends MultiHotRodServersTest {

   private static final String USER_CACHE = "users";
   private static final String ACCOUNT_CACHE = "accounts";

   private static final int NODES = 3;
   private static final int NUM_ENTRIES = 50;

   private RemoteCache<Integer, UserPB> userCache;
   private RemoteCache<Integer, AccountPB> accountCache;

   public Configuration buildIndexedConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.indexing().enable().addProperty("default.directory_provider", "local-heap");
      return builder.build();
   }

   public Configuration getNonIndexLockConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false).build();
   }

   public Configuration getNonIndexDataConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false).build();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      createHotRodServers(NODES, defaultConfiguration);

      cacheManagers.forEach(cm -> {
         cm.defineConfiguration(USER_CACHE, buildIndexedConfig());
         cm.defineConfiguration(ACCOUNT_CACHE, buildIndexedConfig());

         cm.getCache(USER_CACHE);
         cm.getCache(ACCOUNT_CACHE);
      });

      waitForClusterToForm(USER_CACHE, ACCOUNT_CACHE);

      userCache = client(0).getCache(USER_CACHE);
      accountCache = client(0).getCache(ACCOUNT_CACHE);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDomainSCI.INSTANCE;
   }

   @Test
   public void testMassIndexing() {
      getAccountsPB().forEach(a -> accountCache.put(a.getId(), a));
      getUsersPB().forEach(u -> userCache.put(u.getId(), u));

      assertEquals(query(AccountPB.class, accountCache, "description:account1"), 1);
      assertEquals(query(UserPB.class, userCache, "name:name1"), 1);

      reindex(ACCOUNT_CACHE);

      assertEquals(query(AccountPB.class, accountCache, "description:account1"), 1);
      assertEquals(query(UserPB.class, userCache, "name:name1"), 1);

      reindex(USER_CACHE);

      assertEquals(query(AccountPB.class, accountCache, "description:account1"), 1);
      assertEquals(query(UserPB.class, userCache, "name:name1"), 1);

   }

   private void reindex(String cacheName) {
      Cache<?, ?> cache = cacheManagers.get(0).getCache(cacheName);
      MassIndexer massIndexer = org.infinispan.query.Search.getSearchManager(cache).getMassIndexer();
      massIndexer.start();
   }

   private <T> int query(Class<T> entity, RemoteCache<?, ?> cache, String query) {
      String[] fields = query.split(":");
      Query q = Search.getQueryFactory(cache).from(entity).having(fields[0]).eq(fields[1]).build();
      return q.list().size();
   }

   private List<AccountPB> getAccountsPB() {
      return IntStream.range(0, NUM_ENTRIES).boxed().map(i -> {
         AccountPB accountPB = new AccountPB();
         accountPB.setId(i);
         accountPB.setDescription("account" + i);
         accountPB.setCreationDate(new Date());
         return accountPB;
      }).collect(Collectors.toList());
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

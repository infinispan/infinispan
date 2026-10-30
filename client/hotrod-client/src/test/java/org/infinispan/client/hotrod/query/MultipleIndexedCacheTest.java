package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Date;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.Account;
import org.infinispan.protostream.sampledomain.bank.User;
import org.infinispan.query.Indexer;
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

   private RemoteCache<Integer, User> userCache;
   private RemoteCache<Integer, Account> accountCache;

   public Configuration buildIndexedConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(User.ENTITY_NAME)
            .addIndexedEntity(Account.ENTITY_NAME);
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
      for (int i = 0; i < NUM_ENTRIES; i++) {
         Account account = new Account();
         account.setId(i);
         account.setDescription("account" + i);
         account.setCreationDate(new Date());
         accountCache.put(account.getId(), account);

         User user = new User();
         user.setId(i);
         user.setName("name" + i);
         user.setSurname("surname" + i);
         user.setAccountIds(Collections.singleton(i));
         userCache.put(user.getId(), user);
      }
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDomainSCI.INSTANCE;
   }

   @Test
   public void testMassIndexing() {
      assertEquals(1, query(Account.ENTITY_NAME, accountCache, "description", "'account1'"));
      assertEquals(1, query(User.ENTITY_NAME, userCache, "name", "'name1'"));

      reindex(ACCOUNT_CACHE);

      assertEquals(1, query(Account.ENTITY_NAME, accountCache, "description", "'account1'"));
      assertEquals(1, query(User.ENTITY_NAME, userCache, "name", "'name1'"));

      reindex(USER_CACHE);

      assertEquals(1, query(Account.ENTITY_NAME, accountCache, "description", "'account1'"));
      assertEquals(1, query(User.ENTITY_NAME, userCache, "name", "'name1'"));
   }

   @Test
   public void testLocalQueries() {
      Query<?> matchAll = userCache.query("FROM  sample_domain.User");
      long totalUsers = matchAll.execute().count().value();
      assertEquals(NUM_ENTRIES, totalUsers);

      long partialCount = matchAll.local(true).execute().count().value();
      assertTrue(partialCount > 0 && partialCount < NUM_ENTRIES);
   }

   private void reindex(String cacheName) {
      Cache<?, ?> cache = cacheManagers.get(0).getCache(cacheName);
      Indexer indexer = org.infinispan.query.Indexer.of(cache);
      CompletionStages.join(indexer.run());
   }

   private <T> long query(String entity, RemoteCache<?, ?> cache, String fieldName, String fieldValue) {
      Query<T> q = cache.query("FROM " + entity + " WHERE " + fieldName + " = " + fieldValue);
      return q.execute().count().value();
   }
}

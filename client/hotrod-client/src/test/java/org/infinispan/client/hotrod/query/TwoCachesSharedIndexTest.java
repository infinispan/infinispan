package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Date;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.Account;
import org.infinispan.protostream.sampledomain.bank.User;
import org.testng.annotations.Test;

/**
 * Tests remote query using two caches and a shared index
 *
 * @author gustavonalle
 * @since 9.0
 */
@Test(testName = "client.hotrod.query.TwoCachesSharedIndexTest", groups = "functional")
public class TwoCachesSharedIndexTest extends MultiHotRodServersTest {

   private static final String USER_CACHE = "users";
   private static final String ACCOUNT_CACHE = "accounts";

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
      createHotRodServers(2, defaultConfiguration);

      cacheManagers.forEach(cm -> {
         cm.defineConfiguration(USER_CACHE, buildIndexedConfig());
         cm.defineConfiguration(ACCOUNT_CACHE, buildIndexedConfig());

         cm.getCache(USER_CACHE);
         cm.getCache(ACCOUNT_CACHE);
      });

      waitForClusterToForm(USER_CACHE, ACCOUNT_CACHE);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDomainSCI.INSTANCE;
   }

   @Test
   public void testWithUserCache() {
      RemoteCache<Integer, User> userCache = client(0).getCache(USER_CACHE);
      userCache.put(1, getUser());

      Query<User> query = userCache.query("FROM sample_domain.User WHERE name = 'John'");
      List<User> users = query.execute().list();

      assertEquals("John", users.iterator().next().getName());
   }

   @Test
   public void testWithAccountCache() {
      RemoteCache<Integer, Account> accountCache = client(0).getCache(ACCOUNT_CACHE);
      accountCache.put(1, getAccount());

      Query<Account> query = accountCache.query("FROM sample_domain.Account WHERE description = 'account1'");
      List<Account> accounts = query.execute().list();

      assertEquals("account1", accounts.iterator().next().getDescription());
   }

   private Account getAccount() {
      Account accountPB = new Account();
      accountPB.setId(1);
      accountPB.setDescription("account1");
      accountPB.setCreationDate(new Date());
      return accountPB;
   }

   private User getUser() {
      User userPB = new User();
      userPB.setName("John");
      userPB.setSurname("Doe");
      return userPB;
   }
}

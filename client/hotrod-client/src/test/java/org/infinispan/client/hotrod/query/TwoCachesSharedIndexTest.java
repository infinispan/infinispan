package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.List;

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
import org.infinispan.query.dsl.Query;
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
            .addProperty("directory.type", "local-heap");
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
      RemoteCache<Integer, UserPB> userCache = client(0).getCache(USER_CACHE);
      userCache.put(1, getUserPB());

      Query query = Search.getQueryFactory(userCache).from(UserPB.class).having("name").eq("John").build();
      List<UserPB> users = query.list();

      assertEquals("John", users.iterator().next().getName());
   }

   @Test
   public void testWithAccountCache() {
      RemoteCache<Integer, AccountPB> accountCache = client(0).getCache(ACCOUNT_CACHE);
      accountCache.put(1, getAccountPB());

      Query query = Search.getQueryFactory(accountCache).from(AccountPB.class).having("description").eq("account1").build();
      List<AccountPB> accounts = query.list();

      assertEquals(accounts.iterator().next().getDescription(), "account1");
   }

   private AccountPB getAccountPB() {
      AccountPB accountPB = new AccountPB();
      accountPB.setId(1);
      accountPB.setDescription("account1");
      accountPB.setCreationDate(new Date());
      return accountPB;
   }

   private UserPB getUserPB() {
      UserPB userPB = new UserPB();
      userPB.setName("John");
      userPB.setSurname("Doe");
      return userPB;
   }
}

package org.infinispan.query.dsl;

import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.sample_domain_model.Account;
import org.infinispan.query.dsl.sample_domain_model.Address;
import org.infinispan.query.dsl.sample_domain_model.Transaction;
import org.infinispan.query.dsl.sample_domain_model.User;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.dsl.QueryDslTest")
public class QueryDslTest extends SingleCacheManagerTest {

   private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().enable()
            .indexLocalOnly(false)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @BeforeMethod
   private void populateCache() throws Exception {
      // create the test objects
      User user1 = new User();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setAccountIds(new HashSet<Integer>(Arrays.asList(1, 2)));

      Address address1 = new Address();
      address1.setStreet("Main Street");
      address1.setPostCode("X1234");
      user1.setAddresses(Collections.singletonList(address1));

      User user2 = new User();
      user2.setId(2);
      user2.setName("Spider");
      user2.setSurname("Man");
      user2.setAccountIds(Collections.singleton(3));

      Address address2 = new Address();
      address2.setStreet("Old Street");
      address2.setPostCode("Y12");
      Address address3 = new Address();
      address3.setStreet("Bond Street");
      address3.setPostCode("ZZ");
      user2.setAddresses(Arrays.asList(address2, address3));

      Account account1 = new Account();
      account1.setId(1);
      account1.setDescription("John Doe's first bank account");

      Account account2 = new Account();
      account2.setId(2);
      account2.setDescription("John Doe's second bank account");

      Account account3 = new Account();
      account3.setId(3);
      account3.setDescription("Spider Man's bank account");

      Transaction transaction1 = new Transaction();
      transaction1.setId(1);
      transaction1.setDescription("Feb. rent payment");
      transaction1.setAccountId(1);
      transaction1.setAmount(1500);
      transaction1.setDate(dateFormat.parse("2013-01-05"));
      transaction1.setDebit(true);

      Transaction transaction2 = new Transaction();
      transaction2.setId(2);
      transaction2.setDescription("Starbucks");
      transaction2.setAccountId(1);
      transaction2.setAmount(23);
      transaction2.setDate(dateFormat.parse("2013-01-09"));
      transaction2.setDebit(true);

      Transaction transaction3 = new Transaction();
      transaction3.setId(3);
      transaction3.setDescription("Hotel");
      transaction3.setAccountId(2);
      transaction3.setAmount(45);
      transaction3.setDate(dateFormat.parse("2013-02-27"));
      transaction3.setDebit(true);

      // persist and index the test objects
      // we put all of them in the same cache for the sake of simplicity
      cache.put("user_" + user1.getId(), user1);
      cache.put("user_" + user2.getId(), user2);
      cache.put("account_" + account1.getId(), account1);
      cache.put("account_" + account2.getId(), account2);
      cache.put("account_" + account3.getId(), account3);
      cache.put("transaction_" + transaction1.getId(), transaction1);
      cache.put("transaction_" + transaction2.getId(), transaction2);
      cache.put("transaction_" + transaction3.getId(), transaction3);

      SearchFactoryImplementor searchFactory = (SearchFactoryImplementor) Search.getSearchManager(cache).getSearchFactory();
      assertNotNull(searchFactory.getAllIndexesManager().getIndexManager(User.class.getName()));
      assertNotNull(searchFactory.getAllIndexesManager().getIndexManager(Account.class.getName()));
      assertNotNull(searchFactory.getAllIndexesManager().getIndexManager(Transaction.class.getName()));
      assertNull(searchFactory.getAllIndexesManager().getIndexManager(Address.class.getName()));
   }

   public void testQueryWithDsl1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // all users with a given name and surname
      Query q = qf.from(User.class)
            .having("name").eq("John").and().having("surname").eq("Doe")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Doe", list.get(0).getSurname());
   }

   public void testQueryWithDsl2() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // all users in a given post code
      Query q = qf.from(User.class)
            .having("addresses.postCode").eq("X1234")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("X1234", list.get(0).getAddresses().get(0).getPostCode());
   }

   @Test(enabled = false, description = "Like operator not implemented in parser yet")
   public void testQueryWithDsl3() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // all rent payments made from a given account
      Query q = qf.from(Transaction.class)
            .having("accountId").eq(1)
            .and().having("description").like("%rent%") //todo maybe here we should use wildcards like "*rent*" ?
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertEquals(new BigDecimal(1500), list.get(0).getAmount());
   }

   @Test(enabled = false, description = "Date arguments not supported yet")
   public void testQueryWithDsl4() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // all the transactions that happened in January 2013
      Query q = qf.from(Transaction.class)
            .having("accountId").eq(2)
            .and().having("date").between(dateFormat.parse("2013-01-01"), dateFormat.parse("2013-01-31"))
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertTrue(list.get(0).getDate().compareTo(dateFormat.parse("2013-01-31")) < 0);
      assertTrue(list.get(0).getDate().compareTo(dateFormat.parse("2013-01-01")) > 0);
   }

   @Test(enabled = false, description = "Inequality operators not supported yet")
   public void testQueryWithDsl5() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // all the transactions greater than a given amount
      Query q = qf.from(Transaction.class)
            .having("accountId").eq(2)
            .and().having("amount").gt(1000)
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertTrue(list.get(0).getAmount().doubleValue() > 1000);
   }

   public void testQueryWithDsl6() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("name").eq("John")
            .and().having("addresses.postCode").eq("X1234")
            .and(qf.having("accountIds").eq(1))
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Doe", list.get(0).getSurname());
   }
}

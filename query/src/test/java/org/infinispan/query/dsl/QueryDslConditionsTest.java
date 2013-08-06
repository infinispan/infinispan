package org.infinispan.query.dsl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.sample_domain_model.Account;
import org.infinispan.query.dsl.sample_domain_model.Address;
import org.infinispan.query.dsl.sample_domain_model.Transaction;
import org.infinispan.query.dsl.sample_domain_model.User;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for query conditions (filtering).
 *
 * @author anistor@redhat.com
 * @author rvansa@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.dsl.QueryDslConditionsTest")
public class QueryDslConditionsTest extends AbstractQueryDslTest {

   @BeforeMethod
   private void populateCache() throws Exception {
      // create the test objects
      User user1 = new User();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAccountIds(new HashSet<Integer>(Arrays.asList(1, 2)));

      Address address1 = new Address();
      address1.setStreet("Main Street");
      address1.setPostCode("X1234");
      user1.setAddresses(Collections.singletonList(address1));

      User user2 = new User();
      user2.setId(2);
      user2.setName("Spider");
      user2.setSurname("Man");
      user2.setGender(User.Gender.MALE);
      user2.setAccountIds(Collections.singleton(3));

      Address address2 = new Address();
      address2.setStreet("Old Street");
      address2.setPostCode("Y12");
      Address address3 = new Address();
      address3.setStreet("Bond Street");
      address3.setPostCode("ZZ");
      user2.setAddresses(Arrays.asList(address2, address3));

      User user3 = new User();
      user3.setId(3);
      user3.setName("Spider");
      user3.setSurname("Woman");
      user3.setGender(User.Gender.FEMALE);
      user3.setAccountIds(Collections.EMPTY_SET);

      Account account1 = new Account();
      account1.setId(1);
      account1.setDescription("John Doe's first bank account");

      Account account2 = new Account();
      account2.setId(2);
      account2.setDescription("John Doe's second bank account");

      Account account3 = new Account();
      account3.setId(3);
      account3.setDescription("Spider Man's bank account");

      Transaction transaction0 = new Transaction();
      transaction0.setId(0);
      transaction0.setDescription("Birthday present");
      transaction0.setAccountId(1);
      transaction0.setAmount(1800);
      transaction0.setDate(DATE_FORMAT.parse("2012-09-07"));
      transaction0.setDebit(false);

      Transaction transaction1 = new Transaction();
      transaction1.setId(1);
      transaction1.setDescription("Feb. rent payment");
      transaction1.setAccountId(1);
      transaction1.setAmount(1500);
      transaction1.setDate(DATE_FORMAT.parse("2013-01-01"));
      transaction1.setDebit(true);

      Transaction transaction2 = new Transaction();
      transaction2.setId(2);
      transaction2.setDescription("Starbucks");
      transaction2.setAccountId(1);
      transaction2.setAmount(23);
      transaction2.setDate(DATE_FORMAT.parse("2013-01-09"));
      transaction2.setDebit(true);

      Transaction transaction3 = new Transaction();
      transaction3.setId(3);
      transaction3.setDescription("Hotel");
      transaction3.setAccountId(2);
      transaction3.setAmount(45);
      transaction3.setDate(DATE_FORMAT.parse("2013-02-27"));
      transaction3.setDebit(true);

      Transaction transaction4 = new Transaction();
      transaction4.setId(4);
      transaction4.setDescription("Last january");
      transaction4.setAccountId(2);
      transaction4.setAmount(95);
      transaction4.setDate(DATE_FORMAT.parse("2013-01-31"));
      transaction4.setDebit(true);

      // persist and index the test objects
      // we put all of them in the same cache for the sake of simplicity
      cache.put("user_" + user1.getId(), user1);
      cache.put("user_" + user2.getId(), user2);
      cache.put("user_" + user3.getId(), user3);
      cache.put("account_" + account1.getId(), account1);
      cache.put("account_" + account2.getId(), account2);
      cache.put("account_" + account3.getId(), account3);
      cache.put("transaction_" + transaction0.getId(), transaction0);
      cache.put("transaction_" + transaction1.getId(), transaction1);
      cache.put("transaction_" + transaction2.getId(), transaction2);
      cache.put("transaction_" + transaction3.getId(), transaction3);
      cache.put("transaction_" + transaction4.getId(), transaction4);

      SearchFactoryImplementor searchFactory = (SearchFactoryImplementor) Search.getSearchManager(cache).getSearchFactory();
      assertNotNull(searchFactory.getAllIndexesManager().getIndexManager(User.class.getName()));
      assertNotNull(searchFactory.getAllIndexesManager().getIndexManager(Account.class.getName()));
      assertNotNull(searchFactory.getAllIndexesManager().getIndexManager(Transaction.class.getName()));
      assertNull(searchFactory.getAllIndexesManager().getIndexManager(Address.class.getName()));
   }

   public void testEq1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("name").eq("John")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Doe", list.get(0).getSurname());
   }

   public void testEq() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("name").eq("Jacob")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testEqInNested1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // all users in a given post code
      Query q = qf.from(User.class)
            .having("addresses.postCode").eq("X1234")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("X1234", list.get(0).getAddresses().get(0).getPostCode());
   }

   public void testEqInNested2() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("addresses.postCode").eq("Y12")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getAddresses().size());
   }

   @Test(enabled = false, description = "Like operator not implemented in parser yet")
   public void testLike() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // all rent payments made from a given account
      Query q = qf.from(Transaction.class)
            .having("description").like("%rent%") //todo maybe here we should use wildcards like "*rent*" ?
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getAccountId());
      assertEquals(new BigDecimal(1500), list.get(0).getAmount());
   }

   @Test(enabled = false, description = "Date arguments not supported yet")
   public void testBetween1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // all the transactions that happened in January 2013
      Query q = qf.from(Transaction.class)
            .having("date").between(DATE_FORMAT.parse("2013-01-01"), DATE_FORMAT.parse("2013-01-31"))
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(3, list.size());
      for (Transaction t : list) {
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-31")) <= 0);
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-01")) >= 0);
      }
   }

   @Test(enabled = false, description = "Date arguments not supported yet")
   public void testBetween2() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // all the transactions that happened in January 2013
      Query q = qf.from(Transaction.class)
            .having("date").between(DATE_FORMAT.parse("2013-01-01"), DATE_FORMAT.parse("2013-01-31")).includeLower(false)
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(2, list.size());
      for (Transaction t : list) {
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-31")) <= 0);
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-01")) > 0);
      }
   }

   @Test(enabled = false, description = "Date arguments not supported yet")
   public void testBetween3() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // all the transactions that happened in January 2013
      Query q = qf.from(Transaction.class)
            .having("date").between(DATE_FORMAT.parse("2013-01-01"), DATE_FORMAT.parse("2013-01-31")).includeUpper(false)
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(2, list.size());
      for (Transaction t : list) {
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-31")) < 0);
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-01")) >= 0);
      }
   }

   @Test(enabled = false, description = "Inequality operators not supported yet")
   public void testGt() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // all the transactions greater than a given amount
      Query q1 = qf.from(Transaction.class)
            .having("amount").gt(1500)
            .toBuilder().build();

      List<Transaction> list1 = q1.list();
      assertEquals(1, list1.size());
      for (Transaction t : list1) {
         assertTrue(t.getAmount().doubleValue() > 1500);
      }
   }

   @Test(enabled = false, description = "Inequality operators not supported yet")
   public void testGte() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q2 = qf.from(Transaction.class)
            .having("amount").gte(1500)
            .toBuilder().build();

      List<Transaction> list2 = q2.list();
      assertEquals(2, list2.size());
      for (Transaction t : list2) {
         assertTrue(t.getAmount().doubleValue() >= 1500);
      }
   }

   @Test(enabled = false, description = "Inequality operators not supported yet")
   public void testLt() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q3 = qf.from(Transaction.class)
            .having("amount").lt(1500)
            .toBuilder().build();

      List<Transaction> list3 = q3.list();
      assertEquals(3, list3.size());
      for (Transaction t : list3) {
         assertTrue(t.getAmount().doubleValue() < 1500);
      }
   }

   @Test(enabled = false, description = "Inequality operators not supported yet")
   public void testLte() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q4 = qf.from(Transaction.class)
            .having("amount").lte(1500)
            .toBuilder().build();

      List<Transaction> list4 = q4.list();
      assertEquals(4, list4.size());
      for (Transaction t : list4) {
         assertTrue(t.getAmount().doubleValue() <= 1500);
      }
   }

   public void testAnd1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q1 = qf.from(User.class)
            .having("name").eq("Spider")
            .and().having("surname").eq("Man")
            .toBuilder().build();

      List<User> list1 = q1.list();
      assertEquals(1, list1.size());
      assertEquals(2, list1.get(0).getId());
   }

   public void testAnd2() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q2 = qf.from(User.class)
            .having("name").eq("Spider")
            .and(qf.having("surname").eq("Man"))
            .toBuilder().build();

      List<User> list2 = q2.list();
      assertEquals(1, list2.size());
      assertEquals(2, list2.get(0).getId());
   }

   @Test(enabled = false, description = "Enums not supported yet.")
   public void testAnd3() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q3 = qf.from(User.class)
            .having("gender").eq(User.Gender.MALE)
            .and().having("gender").eq(User.Gender.FEMALE)
            .toBuilder().build();

      List<User> list3 = q3.list();
      assertEquals(0, list3.size());
   }

   public void testOr1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q1 = qf.from(User.class)
            .having("surname").eq("Man")
            .or().having("surname").eq("Woman")
            .toBuilder().build();

      List<User> list1 = q1.list();
      assertEquals(2, list1.size());
      for (User u : list1) {
         assertEquals("Spider", u.getName());
      }
   }

   public void testOr2() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q2 = qf.from(User.class)
            .having("surname").eq("Man")
            .or(qf.having("surname").eq("Woman"))
            .toBuilder().build();

      List<User> list2 = q2.list();
      assertEquals(2, list2.size());
      for (User u : list2) {
         assertEquals("Spider", u.getName());
      }
   }

   public void testNot1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .not().having("name").eq("Spider")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
   }

   public void testNot2() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .not().not().having("surname").eq("Doe")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
   }

   public void testNot3() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // NOT should have higher priority than AND
      Query q = qf.from(User.class)
            .not().having("name").eq("John")
            .and().having("surname").eq("Man")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Spider", list.get(0).getName());
   }

   public void testNot4() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // NOT should have higher priority than AND
      Query q = qf.from(User.class)
            .having("surname").eq("Man")
            .and().not().having("name").eq("John")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Spider", list.get(0).getName());
   }

   public void testNot5() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      // NOT should have higher priority than OR
      Query q = qf.from(User.class)
            .not().having("name").eq("Spider")
            .or().having("surname").eq("Man")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      for (User u : list) {
         assertFalse("Woman".equals(u.getSurname()));
      }
   }

   @Test(enabled = false, description = "Enums not supported yet")
   public void testOr3() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q1 = qf.from(User.class)
            .having("gender").eq(User.Gender.MALE)
            .or().having("gender").eq(User.Gender.FEMALE)
            .toBuilder().build();

      List<User> list1 = q1.list();
      assertEquals(3, list1.size());
   }

   public void testEmptyQuery() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class).build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   @Test(enabled = false, description = "isNull not supported yet")
   public void testIsNull() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("addresses").isNull()
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).getId());
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testContains1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("accountIds").contains(2)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testContains2() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("accountIds").contains(42)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testContainsAll1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("accountIds").containsAll(1, 2)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testContainsAll2() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("accountIds").containsAll(Collections.singleton(1))
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testContainsAll3() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("accountIds").containsAll(1, 2, 3)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testContainsAll4() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("accountIds").containsAll(Collections.EMPTY_SET)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testContainsAny1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("accountIds").containsAny(2, 3)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testContainsAny2() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("accountIds").containsAny(4, 5)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testContainsAny3() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("accountIds").containsAny(Collections.EMPTY_SET)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testIn1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      List<Integer> ids = Arrays.asList(1, 3);
      Query q = qf.from(User.class)
            .having("id").in(ids)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      for (User u : list) {
         assertTrue(ids.contains(u.getId()));
      }
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testIn2() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("id").in(4)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   @Test(enabled = false, description = "Collection queries not supported yet")
   public void testIn3() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .having("id").in(Collections.EMPTY_SET)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }
}

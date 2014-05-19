package org.infinispan.query.dsl.embedded;

import org.infinispan.query.Search;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.dsl.embedded.sample_domain_model.Account;
import org.infinispan.query.dsl.embedded.sample_domain_model.Address;
import org.infinispan.query.dsl.embedded.sample_domain_model.Transaction;
import org.infinispan.query.dsl.embedded.sample_domain_model.User;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test for query conditions (filtering) on cache without indexing. Exercises the whole query DSL on the sample domain
 * model.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.dsl.NonIndexedQueryDslConditionsTest")
public class NonIndexedQueryDslConditionsTest extends NonIndexedAbstractQueryDslTest {

   @BeforeMethod(alwaysRun = true)
   protected void populateCache() throws Exception {
      // create the test objects
      User user1 = new User();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAge(22);
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
      user3.setAccountIds(Collections.<Integer>emptySet());

      Account account1 = new Account();
      account1.setId(1);
      account1.setDescription("John Doe's first bank account");
      account1.setCreationDate(DATE_FORMAT.parse("2013-01-03"));

      Account account2 = new Account();
      account2.setId(2);
      account2.setDescription("John Doe's second bank account");
      account2.setCreationDate(DATE_FORMAT.parse("2013-01-04"));

      Account account3 = new Account();
      account3.setId(3);
      account3.setCreationDate(DATE_FORMAT.parse("2013-01-20"));

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
      transaction1.setDate(DATE_FORMAT.parse("2013-01-05"));
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

      Transaction transaction5 = new Transaction();
      transaction5.setId(5);
      transaction5.setDescription("Popcorn");
      transaction5.setAccountId(2);
      transaction5.setAmount(5);
      transaction5.setDate(DATE_FORMAT.parse("2013-01-01"));
      transaction5.setDebit(true);

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
      cache.put("transaction_" + transaction5.getId(), transaction5);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Indexing was not enabled on this cache.*")
   public void testIndexPresence() {
      Search.getSearchManager(cache).getSearchFactory();
   }

   public void testEq1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("name").eq("John")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Doe", list.get(0).getSurname());
   }

   public void testEq() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("name").eq("Jacob")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testEqInNested1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all users in a given post code
      Query q = qf.from(User.class)
            .having("addresses.postCode").eq("X1234")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("X1234", list.get(0).getAddresses().get(0).getPostCode());
   }

   public void testEqInNested2() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("addresses.postCode").eq("Y12")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getAddresses().size());
   }

   public void testLike() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all rent payments made from a given account
      Query q = qf.from(Transaction.class)
            .having("description").like("%rent%")
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getAccountId());
      assertEquals(1500, list.get(0).getAmount(), 0);
   }

   public void testBetween1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all the transactions that happened in January 2013
      Query q = qf.from(Transaction.class)
            .having("date").between(DATE_FORMAT.parse("2013-01-01"), DATE_FORMAT.parse("2013-01-31"))
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(4, list.size());
      for (Transaction t : list) {
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-31")) <= 0);
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-01")) >= 0);
      }
   }

   public void testBetween2() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all the transactions that happened in January 2013
      Query q = qf.from(Transaction.class)
            .having("date").between(DATE_FORMAT.parse("2013-01-01"), DATE_FORMAT.parse("2013-01-31")).includeUpper(false)
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(3, list.size());
      for (Transaction t : list) {
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-31")) < 0);
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-01")) >= 0);
      }
   }

   public void testBetween3() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all the transactions that happened in January 2013
      Query q = qf.from(Transaction.class)
            .having("date").between(DATE_FORMAT.parse("2013-01-01"), DATE_FORMAT.parse("2013-01-31")).includeLower(false)
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(3, list.size());
      for (Transaction t : list) {
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-31")) <= 0);
         assertTrue(t.getDate().compareTo(DATE_FORMAT.parse("2013-01-01")) > 0);
      }
   }

   public void testGt() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all the transactions greater than a given amount
      Query q = qf.from(Transaction.class)
            .having("amount").gt(1500)
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertTrue(list.get(0).getAmount() > 1500);
   }

   public void testGte() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(Transaction.class)
            .having("amount").gte(1500)
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(2, list.size());
      for (Transaction t : list) {
         assertTrue(t.getAmount() >= 1500);
      }
   }

   public void testLt() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(Transaction.class)
            .having("amount").lt(1500)
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(4, list.size());
      for (Transaction t : list) {
         assertTrue(t.getAmount() < 1500);
      }
   }

   public void testLte() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(Transaction.class)
            .having("amount").lte(1500)
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(5, list.size());
      for (Transaction t : list) {
         assertTrue(t.getAmount() <= 1500);
      }
   }

   public void testAnd1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("name").eq("Spider")
            .and().having("surname").eq("Man")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getId());
   }

   public void testAnd2() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("name").eq("Spider")
            .and(qf.having("surname").eq("Man"))
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getId());
   }

   public void testAnd3() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("gender").eq(User.Gender.MALE)
            .and().having("gender").eq(User.Gender.FEMALE)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testAnd4() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      //test for parenthesis, "and" should have higher priority
      Query q = qf.from(User.class)
            .having("name").eq("Spider")
            .or(qf.having("name").eq("John"))
            .and(qf.having("surname").eq("Man"))
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
   }

   public void testOr1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("surname").eq("Man")
            .or().having("surname").eq("Woman")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      for (User u : list) {
         assertEquals("Spider", u.getName());
      }
   }

   public void testOr2() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("surname").eq("Man")
            .or(qf.having("surname").eq("Woman"))
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      for (User u : list) {
         assertEquals("Spider", u.getName());
      }
   }

   public void testOr3() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("gender").eq(User.Gender.MALE)
            .or().having("gender").eq(User.Gender.FEMALE)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testOr4() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("gender").eq(User.Gender.MALE)
            .or().having("name").eq("Spider")
            .and().having("gender").eq(User.Gender.FEMALE)
            .or().having("surname").like("%oe%")
            .toBuilder().build();
      List<User> list = q.list();

      assertEquals(2, list.size());
      assertEquals("Woman", list.get(0).getSurname());
      assertEquals("Doe", list.get(1).getSurname());
   }

   public void testOr5() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("gender").eq(User.Gender.MALE)
            .or().having("name").eq("Spider")
            .or().having("gender").eq(User.Gender.FEMALE)
            .and().having("surname").like("%oe%")
            .toBuilder().build();
      List<User> list = q.list();

      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
   }

   public void testNot1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .not().having("name").eq("Spider")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
   }

   public void testNot2() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .not().not().having("surname").eq("Doe")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
   }

   public void testNot3() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

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
      QueryFactory qf = Search.getQueryFactory(cache);

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
      QueryFactory qf = Search.getQueryFactory(cache);

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

   public void testNot6() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // QueryFactory.not() test
      Query q = qf.from(User.class)
            .not(qf.not(qf.having("gender").eq(User.Gender.FEMALE)))
            .toBuilder()
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertTrue(list.get(0).getSurname().equals("Woman"));
   }

   public void testNot7() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("gender").eq(User.Gender.FEMALE)
            .and().not(qf.having("name").eq("Spider"))
            .toBuilder().build();

      List<User> list = q.list();
      assertTrue(list.isEmpty());
   }

   public void testEmptyQuery() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class).build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testIsNull() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("addresses").isNull()
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).getId());
   }

   public void testContains1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("accountIds").contains(2)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testContains2() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("accountIds").contains(42)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testContainsAll1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("accountIds").containsAll(1, 2)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testContainsAll2() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("accountIds").containsAll(Collections.singleton(1))
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testContainsAll3() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("accountIds").containsAll(1, 2, 3)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testContainsAll4() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("accountIds").containsAll(Collections.emptySet())
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testContainsAny1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("accountIds").containsAny(2, 3)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());

      List<Integer> ids = new ArrayList<Integer>();
      for (User u : list) {
         ids.add(u.getId());
      }
      assertTrue(ids.contains(1));
      assertTrue(ids.contains(2));
   }

   public void testContainsAny2() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("accountIds").containsAny(4, 5)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testContainsAny3() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("accountIds").containsAny(Collections.emptySet())
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testIn1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

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

   public void testIn2() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("id").in(4)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIn3() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      qf.from(User.class).having("id").in(Collections.emptySet());
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIn4() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Collection collection = null;
      qf.from(User.class).having("id").in(collection);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIn5() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Object[] array = null;
      qf.from(User.class).having("id").in(array);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIn6() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Object[] array = new Object[0];
      qf.from(User.class).having("id").in(array);
   }

   @Test(enabled = false)  //todo
   public void testSampleDomainQuery1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all male users
      Query q = qf.from(User.class)
            .having("gender").eq(User.Gender.MALE)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
   }

   @Test(enabled = false) //todo
   public void testSampleDomainQuery2() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all male users, but this time retrieved in a twisted manner
      Query q = qf.from(User.class)
            .not(qf.having("gender").eq(User.Gender.FEMALE))
            .and(qf.not().not(qf.having("gender").eq(User.Gender.MALE)))
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
   }

   @Test(enabled = false, description = "String literal escaping is not properly done yet, see ISPN-4045")
   //todo [anistor] fix disabled test
   public void testStringEscape() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all transactions that have a given description. the description contains characters that need to be escaped.
      Query q = qf.from(Account.class)
            .having("description").eq("John Doe's first bank account")
            .toBuilder().build();

      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getId());
   }

   @Test(enabled = false)    //todo
   public void testSortByDate() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(Account.class)
            .orderBy("creationDate", SortOrder.DESC)
            .build();

      List<Account> list = q.list();
      assertEquals(3, list.size());
      assertEquals(3, list.get(0).getId());
      assertEquals(2, list.get(1).getId());
      assertEquals(1, list.get(2).getId());
   }

   @Test(enabled = false) //todo
   public void testSampleDomainQuery3() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all male users
      Query q = qf.from(User.class)
            .having("gender").eq(User.Gender.MALE)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
   }

   @Test(enabled = false) //todo
   public void testSampleDomainQuery4() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all users ordered descendingly by name
      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.DESC)
            .build();

      List<User> list = q.list();
      assertEquals(3, list.size());
      assertEquals("Spider", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
      assertEquals("John", list.get(2).getName());
   }

   @Test(enabled = false) //todo
   public void testSampleDomainQuery4With2SortingOptions() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all users ordered descendingly by name
      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.DESC)
            .orderBy("surname", SortOrder.ASC)
            .build();

      List<User> list = q.list();
      assertEquals(3, list.size());

      assertEquals("Spider", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
      assertEquals("John", list.get(2).getName());

      assertEquals("Man", list.get(0).getSurname());
      assertEquals("Woman", list.get(1).getSurname());
      assertEquals("Doe", list.get(2).getSurname());
   }

   @Test(enabled = false) //todo
   public void testSampleDomainQuery5() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // name projection of all users ordered descendingly by name
      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.DESC)
            .setProjection("name")
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
      assertEquals(1, list.get(2).length);
      assertEquals("Spider", list.get(0)[0]);
      assertEquals("Spider", list.get(1)[0]);
      assertEquals("John", list.get(2)[0]);
   }

   public void testSampleDomainQuery6() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all users with a given name and surname
      Query q = qf.from(User.class)
            .having("name").eq("John")
            .and().having("surname").eq("Doe")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Doe", list.get(0).getSurname());
   }

   public void testSampleDomainQuery7() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all rent payments made from a given account
      Query q = qf.from(Transaction.class)
            .having("accountId").eq(1)
            .and().having("description").like("%rent%")
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
      assertEquals(1, list.get(0).getAccountId());
      assertTrue(list.get(0).getDescription().contains("rent"));
   }

   @Test(enabled = false) //todo
   public void testSampleDomainQuery8() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all the transactions that happened in January 2013
      Query q = qf.from(Transaction.class)
            .having("date").between(DATE_FORMAT.parse("2013-01-01"), DATE_FORMAT.parse("2013-01-31"))
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(4, list.size());
      assertTrue(list.get(0).getDate().compareTo(DATE_FORMAT.parse("2013-01-31")) < 0);
      assertTrue(list.get(0).getDate().compareTo(DATE_FORMAT.parse("2013-01-01")) > 0);
      assertTrue(list.get(1).getDate().compareTo(DATE_FORMAT.parse("2013-01-31")) < 0);
      assertTrue(list.get(1).getDate().compareTo(DATE_FORMAT.parse("2013-01-01")) > 0);
   }

   public void testSampleDomainQuery9() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all the transactions that happened in January 2013, projected by date field only
      Query q = qf.from(Transaction.class)
            .setProjection("date")
            .having("date").between(DATE_FORMAT.parse("2013-01-01"), DATE_FORMAT.parse("2013-01-31"))
            .toBuilder().build();

      List<Object[]> list = q.list();
      assertEquals(4, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
      assertEquals(1, list.get(2).length);
      assertEquals(1, list.get(3).length);

      for (int i = 0; i < 4; i++) {
         Date d = (Date) list.get(i)[0];
         assertTrue(d.compareTo(DATE_FORMAT.parse("2013-01-31")) <= 0);
         assertTrue(d.compareTo(DATE_FORMAT.parse("2013-01-01")) >= 0);
      }
   }

   public void testSampleDomainQuery10() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all the transactions for a an account having amount greater than a given amount
      Query q = qf.from(Transaction.class)
            .having("accountId").eq(2)
            .and().having("amount").gt(40)
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(2, list.size());
      assertTrue(list.get(0).getAmount() > 40);
      assertTrue(list.get(1).getAmount() > 40);
   }

   public void testSampleDomainQuery11() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("name").eq("John")
            .and().having("addresses.postCode").eq("X1234")
            .and(qf.having("accountIds").eq(1))
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Doe", list.get(0).getSurname());
   }

   public void testSampleDomainQuery12() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all the transactions that represents credits to the account
      Query q = qf.from(Transaction.class)
            .having("accountId").eq(1)
            .and()
            .not().having("isDebit").eq(true).toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertFalse(list.get(0).isDebit());
   }

   public void testSampleDomainQuery13() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // the user that has the bank account with id 3
      Query q = qf.from(User.class)
            .having("accountIds").contains(3).toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getId());
      assertTrue(list.get(0).getAccountIds().contains(3));
   }

   public void testSampleDomainQuery14() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // the user that has all the specified bank accounts
      Query q = qf.from(User.class)
            .having("accountIds").containsAll(2, 1).toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
      assertTrue(list.get(0).getAccountIds().contains(1));
      assertTrue(list.get(0).getAccountIds().contains(2));
   }

   public void testSampleDomainQuery15() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // the user that has at least one of the specified accounts
      Query q = qf.from(User.class)
            .having("accountIds").containsAny(1, 3).toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 2).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 2).contains(list.get(1).getId()));
   }

   @Test(enabled = false) //todo
   public void testSampleDomainQuery16() throws Exception {
      for (int i = 0; i < 50; i++) {
         Transaction transaction = new Transaction();
         transaction.setId(50 + i);
         transaction.setDescription("Expensive shoes " + i);
         transaction.setAccountId(2);
         transaction.setAmount(100 + i);
         transaction.setDate(DATE_FORMAT.parse("2013-08-20"));
         transaction.setDebit(true);
         cache.put("transaction_" + transaction.getId(), transaction);
      }

      QueryFactory qf = Search.getQueryFactory(cache);

      // third batch of 10 transactions for a given account
      Query q = qf.from(Transaction.class)
            .startOffset(20).maxResults(10)
            .orderBy("id", SortOrder.ASC)
            .having("accountId").eq(2).and().having("description").like("Expensive%")
            .toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(50, q.getResultSize());
      assertEquals(10, list.size());
      for (int i = 0; i < 10; i++) {
         assertEquals("Expensive shoes " + (20 + i), list.get(i).getDescription());
      }
   }

   @Test(enabled = false) //todo
   public void testSampleDomainQuery17() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all accounts for a user. first get the user by id and then get his account.
      Query q1 = qf.from(User.class)
            .having("id").eq(1).toBuilder().build();

      List<User> users = q1.list();
      Query q2 = qf.from(Account.class)
            .orderBy("description", SortOrder.ASC)
            .having("id").in(users.get(0).getAccountIds()).toBuilder().build();

      List<Account> list = q2.list();
      assertEquals(2, list.size());
      assertEquals("John Doe's first bank account", list.get(0).getDescription());
      assertEquals("John Doe's second bank account", list.get(1).getDescription());
   }

   public void testSampleDomainQuery18() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      // all transactions of account with id 2 which have an amount larger than 1600 or their description contains the word 'rent'
      Query q = qf.from(Transaction.class)
            .having("accountId").eq(1)
            .and(qf.having("amount").gt(1600)
                       .or().having("description").like("%rent%")).toBuilder().build();

      List<Transaction> list = q.list();
      assertEquals(2, list.size());
      List<String> descriptions = new ArrayList<String>();
      for (Transaction t : list) {
         descriptions.add(t.getDescription());
      }
      assertTrue(descriptions.contains("Birthday present"));
      assertTrue(descriptions.contains("Feb. rent payment"));
   }

   @Test(enabled = false) //todo
   public void testProjectionOnOptionalField() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .setProjection("id", "addresses.postCode")
            .orderBy("id", SortOrder.ASC)
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(1, list.get(0)[0]);
      assertEquals("X1234", list.get(0)[1]);
      assertEquals(2, list.get(1)[0]);
      assertEquals("Y12", list.get(1)[1]);
      assertEquals(3, list.get(2)[0]);
      assertNull(list.get(2)[1]);
   }

   public void testNullOnIntegerField() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("age").isNull()
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
   }

   @Test(enabled = false) //todo
   public void testSampleDomainQuery19() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("addresses.postCode").in("ZZ", "X1234").toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 2).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 2).contains(list.get(1).getId()));
   }

   @Test(enabled = false) //todo
   public void testSampleDomainQuery20() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .not().having("addresses.postCode").in("X1234").toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(2, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(2, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery21() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .not().having("addresses").isNull().toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 2).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 2).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery22() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .not().having("addresses.postCode").like("%123%").toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(2, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(2, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery23() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .not().having("id").between(1, 2)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).getId());
   }

   public void testSampleDomainQuery24() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .not().having("id").between(1, 2).includeLower(false)
            .toBuilder().build();

      List<User> list = q.list();

      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery25() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .not().having("id").between(1, 2).includeUpper(false)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(2, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(2, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery26() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(Account.class)
            .having("creationDate").eq(DATE_FORMAT.parse("2013-01-20"))
            .toBuilder().build();

      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).getId());
   }

   public void testSampleDomainQuery27() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(Account.class)
            .having("creationDate").lt(DATE_FORMAT.parse("2013-01-20"))
            .toBuilder().build();

      List<Account> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).getId());
      assertEquals(2, list.get(1).getId());
   }

   @Test(enabled = false) //todo
   public void testSampleDomainQuery28() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(Account.class)
            .having("creationDate").lte(DATE_FORMAT.parse("2013-01-20"))
            .toBuilder().build();

      List<Account> list = q.list();
      assertEquals(3, list.size());
      assertEquals(1, list.get(0).getId());
      assertEquals(2, list.get(1).getId());
      assertEquals(3, list.get(2).getId());
   }

   public void testSampleDomainQuery29() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(Account.class)
            .having("creationDate").gt(DATE_FORMAT.parse("2013-01-04"))
            .toBuilder().build();

      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).getId());
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding1() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.not().having("name").eq("John").toBuilder().build();
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding2() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("name").eq("John").toBuilder()
            .having("surname").eq("Man").toBuilder()
            .build();
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding3() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .not().having("name").eq("John").toBuilder()
            .not().having("surname").eq("Man").toBuilder()
            .build();
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding4() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .not(qf.having("name").eq("John")).toBuilder()
            .not(qf.having("surname").eq("Man")).toBuilder()
            .build();
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding5() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .not(qf.having("name").eq("John")).toBuilder()
            .not(qf.having("surname").eq("Man")).toBuilder()
            .build();
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testWrongQueryBuilding6() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      Query q = qf.from(User.class)
            .having("gender").eq(null)
            .toBuilder().build();
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding7() throws Exception {
      QueryFactory qf = Search.getQueryFactory(cache);

      FilterConditionEndContext q1 = qf.from(User.class)
            .having("gender");

      q1.eq(User.Gender.MALE);
      q1.eq(User.Gender.FEMALE);
   }
}

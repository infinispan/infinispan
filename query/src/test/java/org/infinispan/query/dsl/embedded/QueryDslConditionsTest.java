package org.infinispan.query.dsl.embedded;

import static org.infinispan.query.dsl.Expression.avg;
import static org.infinispan.query.dsl.Expression.count;
import static org.infinispan.query.dsl.Expression.max;
import static org.infinispan.query.dsl.Expression.min;
import static org.infinispan.query.dsl.Expression.param;
import static org.infinispan.query.dsl.Expression.property;
import static org.infinispan.query.dsl.Expression.sum;
import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test for query conditions (filtering). Exercises the whole query DSL on the sample domain model. Uses indexing,
 * although some fields are not indexed in order to test hybrid queries too.
 *
 * @author anistor@redhat.com
 * @author rvansa@redhat.com
 * @author jmarkos@redhat.com
 * @since 6.0
 */
@Test(groups = {"functional", "smoke"}, testName = "query.dsl.embedded.QueryDslConditionsTest")
public class QueryDslConditionsTest extends AbstractQueryDslTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().index(Index.ALL)
            .addIndexedEntity(getModelFactory().getUserImplClass())
            .addIndexedEntity(getModelFactory().getAccountImplClass())
            .addIndexedEntity(getModelFactory().getTransactionImplClass())
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(1, cfg);
   }

   protected boolean testNullCollections() {
      return true;
   }

   @BeforeClass(alwaysRun = true)
   protected void populateCache() throws Exception {
      // create the test objects
      User user1 = getModelFactory().makeUser();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAge(22);
      user1.setAccountIds(new HashSet<>(Arrays.asList(1, 2)));
      user1.setNotes("Lorem ipsum dolor sit amet");
      user1.setCreationDate(Instant.parse("2011-12-03T10:15:30Z"));
      user1.setPasswordExpirationDate(Instant.parse("2011-12-03T10:15:30Z"));

      Address address1 = getModelFactory().makeAddress();
      address1.setStreet("Main Street");
      address1.setPostCode("X1234");
      address1.setNumber(156);
      user1.setAddresses(Collections.singletonList(address1));

      User user2 = getModelFactory().makeUser();
      user2.setId(2);
      user2.setName("Spider");
      user2.setSurname("Man");
      user2.setSalutation("Mr.");
      user2.setGender(User.Gender.MALE);
      user2.setAccountIds(Collections.singleton(3));
      user2.setCreationDate(Instant.parse("2011-12-03T10:15:30Z"));
      user2.setPasswordExpirationDate(Instant.parse("2011-12-03T10:15:30Z"));

      Address address2 = getModelFactory().makeAddress();
      address2.setStreet("Old Street");
      address2.setPostCode("Y12");
      address2.setNumber(-12);
      Address address3 = getModelFactory().makeAddress();
      address3.setStreet("Bond Street");
      address3.setPostCode("ZZ");
      address3.setNumber(312);
      user2.setAddresses(Arrays.asList(address2, address3));

      User user3 = getModelFactory().makeUser();
      user3.setId(3);
      user3.setName("Spider");
      user3.setSurname("Woman");
      user3.setSalutation("Ms.");
      user3.setGender(User.Gender.FEMALE);
      user3.setAccountIds(Collections.emptySet());
      user3.setCreationDate(Instant.parse("2011-12-03T10:15:30Z"));
      user3.setPasswordExpirationDate(Instant.parse("2011-12-03T10:15:30Z"));
      if (!testNullCollections()) {
         user3.setAddresses(new ArrayList<>());
      }

      Account account1 = getModelFactory().makeAccount();
      account1.setId(1);
      account1.setDescription("John Doe's first bank account");
      account1.setCreationDate(makeDate("2013-01-03"));

      Account account2 = getModelFactory().makeAccount();
      account2.setId(2);
      account2.setDescription("John Doe's second bank account");
      account2.setCreationDate(makeDate("2013-01-04"));

      Account account3 = getModelFactory().makeAccount();
      account3.setId(3);
      account3.setCreationDate(makeDate("2013-01-20"));

      Transaction transaction0 = getModelFactory().makeTransaction();
      transaction0.setId(0);
      transaction0.setDescription("Birthday present");
      transaction0.setAccountId(1);
      transaction0.setAmount(1800);
      transaction0.setDate(makeDate("2012-09-07"));
      transaction0.setDebit(false);
      transaction0.setValid(true);

      Transaction transaction1 = getModelFactory().makeTransaction();
      transaction1.setId(1);
      transaction1.setDescription("Feb. rent payment");
      transaction1.setLongDescription("Feb. rent payment");
      transaction1.setAccountId(1);
      transaction1.setAmount(1500);
      transaction1.setDate(makeDate("2013-01-05"));
      transaction1.setDebit(true);
      transaction1.setValid(true);

      Transaction transaction2 = getModelFactory().makeTransaction();
      transaction2.setId(2);
      transaction2.setDescription("Starbucks");
      transaction2.setLongDescription("Starbucks");
      transaction2.setAccountId(1);
      transaction2.setAmount(23);
      transaction2.setDate(makeDate("2013-01-09"));
      transaction2.setDebit(true);
      transaction2.setValid(true);

      Transaction transaction3 = getModelFactory().makeTransaction();
      transaction3.setId(3);
      transaction3.setDescription("Hotel");
      transaction3.setAccountId(2);
      transaction3.setAmount(45);
      transaction3.setDate(makeDate("2013-02-27"));
      transaction3.setDebit(true);
      transaction3.setValid(true);

      Transaction transaction4 = getModelFactory().makeTransaction();
      transaction4.setId(4);
      transaction4.setDescription("Last january");
      transaction4.setLongDescription("Last january");
      transaction4.setAccountId(2);
      transaction4.setAmount(95);
      transaction4.setDate(makeDate("2013-01-31"));
      transaction4.setDebit(true);
      transaction4.setValid(true);

      Transaction transaction5 = getModelFactory().makeTransaction();
      transaction5.setId(5);
      transaction5.setDescription("-Popcorn");
      transaction5.setLongDescription("-Popcorn");
      transaction5.setAccountId(2);
      transaction5.setAmount(5);
      transaction5.setDate(makeDate("2013-01-01"));
      transaction5.setDebit(true);
      transaction5.setValid(true);

      // persist and index the test objects
      // we put all of them in the same cache for the sake of simplicity
      getCacheForWrite().put("user_" + user1.getId(), user1);
      getCacheForWrite().put("user_" + user2.getId(), user2);
      getCacheForWrite().put("user_" + user3.getId(), user3);
      getCacheForWrite().put("account_" + account1.getId(), account1);
      getCacheForWrite().put("account_" + account2.getId(), account2);
      getCacheForWrite().put("account_" + account3.getId(), account3);
      getCacheForWrite().put("transaction_" + transaction0.getId(), transaction0);
      getCacheForWrite().put("transaction_" + transaction1.getId(), transaction1);
      getCacheForWrite().put("transaction_" + transaction2.getId(), transaction2);
      getCacheForWrite().put("transaction_" + transaction3.getId(), transaction3);
      getCacheForWrite().put("transaction_" + transaction4.getId(), transaction4);
      getCacheForWrite().put("transaction_" + transaction5.getId(), transaction5);

      for (int i = 0; i < 50; i++) {
         Transaction transaction = getModelFactory().makeTransaction();
         transaction.setId(50 + i);
         transaction.setDescription("Expensive shoes " + i);
         transaction.setLongDescription("Expensive shoes " + i);
         transaction.setAccountId(2);
         transaction.setAmount(100 + i);
         transaction.setDate(makeDate("2013-08-20"));
         transaction.setDebit(true);
         transaction.setValid(true);
         getCacheForWrite().put("transaction_" + transaction.getId(), transaction);
      }

      // this value should be ignored gracefully for indexing and querying because primitives are not currently supported
      getCacheForWrite().put("dummy", "a primitive value cannot be queried");

      getCacheForWrite().put("notIndexed1", new NotIndexed("testing 123"));
      getCacheForWrite().put("notIndexed2", new NotIndexed("xyz"));
   }

   public void testIndexPresence() {
      SearchIntegrator searchIntegrator = Search.getSearchManager((Cache) getCacheForQuery()).unwrap(SearchIntegrator.class);

      verifyClassIsIndexed(searchIntegrator, getModelFactory().getUserImplClass());
      verifyClassIsIndexed(searchIntegrator, getModelFactory().getAccountImplClass());
      verifyClassIsIndexed(searchIntegrator, getModelFactory().getTransactionImplClass());
      verifyClassIsNotIndexed(searchIntegrator, getModelFactory().getAddressImplClass());
   }

   private void verifyClassIsNotIndexed(SearchIntegrator searchIntegrator, Class<?> type) {
      assertFalse(searchIntegrator.getIndexBindings().containsKey(PojoIndexedTypeIdentifier.convertFromLegacy(type)));
      assertNull(searchIntegrator.getIndexManager(type.getName()));
   }

   private void verifyClassIsIndexed(SearchIntegrator searchIntegrator, Class<?> type) {
      assertTrue(searchIntegrator.getIndexBindings().containsKey(PojoIndexedTypeIdentifier.convertFromLegacy(type)));
      assertNotNull(searchIntegrator.getIndexManager(type.getName()));
   }

   public void testQueryFactoryType() {
      assertEquals(EmbeddedQueryFactory.class, getQueryFactory().getClass());
   }

   public void testEq1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("John")
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Doe", list.get(0).getSurname());
   }

   public void testEqEmptyString() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("")
            .build();

      List<User> list = q.list();
      assertTrue(list.isEmpty());
   }

   public void testEqSentence() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getAccountImplClass())
            .having("description").eq("John Doe's first bank account")
            .build();

      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testEq() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("Jacob")
            .build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testEqNonIndexedType() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(NotIndexed.class)
            .having("notIndexedField").eq("testing 123")
            .build();

      List<NotIndexed> list = q.list();
      assertEquals(1, list.size());
      assertEquals("testing 123", list.get(0).notIndexedField);
   }

   public void testEqNonIndexedField() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("notes").eq("Lorem ipsum dolor sit amet")
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testEqHybridQuery() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("notes").eq("Lorem ipsum dolor sit amet")
            .and().having("surname").eq("Doe")
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testEqHybridQueryWithParam() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("notes").eq("Lorem ipsum dolor sit amet")
            .and().having("surname").eq(param("surnameParam"))
            .build();

      q.setParameter("surnameParam", "Doe");

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testEqHybridQueryWithPredicateOptimisation() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("notes").like("%ipsum%")
            .and(qf.having("name").eq("John").or().having("name").eq("Jane"))
            .build();

      List<User> list = q.list();

      assertEquals(1, list.size());
      assertEquals("Lorem ipsum dolor sit amet", list.get(0).getNotes());
   }

   public void testEqInNested1() {
      QueryFactory qf = getQueryFactory();

      // all users in a given post code
      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("addresses.postCode").eq("X1234")
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("X1234", list.get(0).getAddresses().get(0).getPostCode());
   }

   public void testEqInNested2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("addresses.postCode").eq("Y12")
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getAddresses().size());
   }

   public void testLike() {
      QueryFactory qf = getQueryFactory();

      // all rent payments made from a given account
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("description").like("%rent%")
            .build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getAccountId());
      assertEquals(1500, list.get(0).getAmount(), 0);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014802: 'from' must be an instance of java.lang.Comparable")
   public void testBetweenArgsAreComparable() {
      QueryFactory qf = getQueryFactory();

      qf.from(getModelFactory().getTransactionImplClass())
            .having("date").between(new Object(), new Object())
            .build();
   }

   public void testBetween1() throws Exception {
      QueryFactory qf = getQueryFactory();

      // all the transactions that happened in January 2013
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("date").between(makeDate("2013-01-01"), makeDate("2013-01-31"))
            .build();

      List<Transaction> list = q.list();
      assertEquals(4, list.size());
      for (Transaction t : list) {
         assertTrue(t.getDate().compareTo(makeDate("2013-01-31")) <= 0);
         assertTrue(t.getDate().compareTo(makeDate("2013-01-01")) >= 0);
      }
   }

   public void testBetween2() throws Exception {
      QueryFactory qf = getQueryFactory();

      // all the transactions that happened in January 2013
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("date").between(makeDate("2013-01-01"), makeDate("2013-01-31")).includeUpper(false)
            .build();

      List<Transaction> list = q.list();
      assertEquals(3, list.size());
      for (Transaction t : list) {
         assertTrue(t.getDate().compareTo(makeDate("2013-01-31")) < 0);
         assertTrue(t.getDate().compareTo(makeDate("2013-01-01")) >= 0);
      }
   }

   public void testBetween3() throws Exception {
      QueryFactory qf = getQueryFactory();

      // all the transactions that happened in January 2013
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("date").between(makeDate("2013-01-01"), makeDate("2013-01-31")).includeLower(false)
            .build();

      List<Transaction> list = q.list();
      assertEquals(3, list.size());
      for (Transaction t : list) {
         assertTrue(t.getDate().compareTo(makeDate("2013-01-31")) <= 0);
         assertTrue(t.getDate().compareTo(makeDate("2013-01-01")) > 0);
      }
   }

   public void testGt() {
      QueryFactory qf = getQueryFactory();

      // all the transactions greater than a given amount
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("amount").gt(1500)
            .build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertTrue(list.get(0).getAmount() > 1500);
   }

   public void testGte() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("amount").gte(1500)
            .build();

      List<Transaction> list = q.list();
      assertEquals(2, list.size());
      for (Transaction t : list) {
         assertTrue(t.getAmount() >= 1500);
      }
   }

   public void testLt() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("amount").lt(1500)
            .build();

      List<Transaction> list = q.list();
      assertEquals(54, list.size());
      for (Transaction t : list) {
         assertTrue(t.getAmount() < 1500);
      }
   }

   public void testLte() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("amount").lte(1500)
            .build();

      List<Transaction> list = q.list();
      assertEquals(55, list.size());
      for (Transaction t : list) {
         assertTrue(t.getAmount() <= 1500);
      }
   }

   // This tests against https://hibernate.atlassian.net/browse/HSEARCH-2030
   public void testLteOnFieldWithNullToken() {
      QueryFactory qf = getQueryFactory();

      // all the transactions that happened in January 2013
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("description").lte("-Popcorn")
            .build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertEquals("-Popcorn", list.get(0).getDescription());
   }

   public void testAnd1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("Spider")
            .and().having("surname").eq("Man")
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getId());
   }

   public void testAnd2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("Spider")
            .and(qf.having("surname").eq("Man"))
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getId());
   }

   public void testAnd3() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("gender").eq(User.Gender.MALE)
            .and().having("gender").eq(User.Gender.FEMALE)
            .build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testAnd4() {
      QueryFactory qf = getQueryFactory();

      //test for parenthesis, "and" should have higher priority
      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("Spider")
            .or(qf.having("name").eq("John"))
            .and(qf.having("surname").eq("Man"))
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
   }

   public void testOr1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("surname").eq("Man")
            .or().having("surname").eq("Woman")
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      for (User u : list) {
         assertEquals("Spider", u.getName());
      }
   }

   public void testOr2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("surname").eq("Man")
            .or(qf.having("surname").eq("Woman"))
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      for (User u : list) {
         assertEquals("Spider", u.getName());
      }
   }

   public void testOr3() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("gender").eq(User.Gender.MALE)
            .or().having("gender").eq(User.Gender.FEMALE)
            .build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testOr4() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("surname", SortOrder.DESC)
            .having("gender").eq(User.Gender.MALE)
            .or().having("name").eq("Spider")
            .and().having("gender").eq(User.Gender.FEMALE)
            .or().having("surname").like("%oe%")
            .build();
      List<User> list = q.list();

      assertEquals(2, list.size());
      assertEquals("Woman", list.get(0).getSurname());
      assertEquals("Doe", list.get(1).getSurname());
   }

   public void testOr5() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("gender").eq(User.Gender.MALE)
            .or().having("name").eq("Spider")
            .or().having("gender").eq(User.Gender.FEMALE)
            .and().having("surname").like("%oe%")
            .build();
      List<User> list = q.list();

      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
   }

   public void testNot1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("name").eq("Spider")
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
   }

   public void testNot2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().not().having("surname").eq("Doe")
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
   }

   public void testNot3() {
      QueryFactory qf = getQueryFactory();

      // NOT should have higher priority than AND
      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("name").eq("John")
            .and().having("surname").eq("Man")
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Spider", list.get(0).getName());
   }

   public void testNot4() {
      QueryFactory qf = getQueryFactory();

      // NOT should have higher priority than AND
      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("surname").eq("Man")
            .and().not().having("name").eq("John")
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Spider", list.get(0).getName());
   }

   public void testNot5() {
      QueryFactory qf = getQueryFactory();

      // NOT should have higher priority than OR
      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("name").eq("Spider")
            .or().having("surname").eq("Man")
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      for (User u : list) {
         assertFalse("Woman".equals(u.getSurname()));
      }
   }

   public void testNot6() {
      QueryFactory qf = getQueryFactory();

      // QueryFactory.not() test
      Query q = qf.from(getModelFactory().getUserImplClass())
            .not(qf.not(qf.having("gender").eq(User.Gender.FEMALE)))
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Woman", list.get(0).getSurname());
   }

   public void testNot7() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("gender").eq(User.Gender.FEMALE)
            .and().not(qf.having("name").eq("Spider"))
            .build();

      List<User> list = q.list();
      assertTrue(list.isEmpty());
   }

   public void testNot8() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not(
                  qf.having("name").eq("John")
                        .or(qf.having("surname").eq("Man")))
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Spider", list.get(0).getName());
      assertEquals("Woman", list.get(0).getSurname());
   }

   public void testNot9() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not(
                  qf.having("name").eq("John")
                        .and(qf.having("surname").eq("Doe")))
            .orderBy("id", SortOrder.ASC)
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals("Spider", list.get(0).getName());
      assertEquals("Man", list.get(0).getSurname());
      assertEquals("Spider", list.get(1).getName());
      assertEquals("Woman", list.get(1).getSurname());
   }

   public void testNot10() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().not(
                  qf.having("name").eq("John")
                        .or(qf.having("surname").eq("Man")))
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertNotEquals("Woman", list.get(0).getSurname());
   }

   public void testNot11() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not(qf.not(
                  qf.having("name").eq("John")
                        .or(qf.having("surname").eq("Man"))))
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertNotEquals("Woman", list.get(0).getSurname());
   }

   public void testEmptyQuery() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass()).build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testTautology() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").gt("A").or().having("name").lte("A")
            .build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testContradiction() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").gt("A").and().having("name").lte("A")
            .build();

      List<User> list = q.list();
      assertTrue(list.isEmpty());
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028503:.*")
   public void testInvalidEmbeddedAttributeQuery() {
      QueryFactory qf = getQueryFactory();

      QueryBuilder queryBuilder = qf.from(getModelFactory().getUserImplClass())
            .select("addresses");

      Query q = queryBuilder.build();

      q.list();  // exception expected
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014027: The property path 'addresses.postCode' cannot be projected because it is multi-valued")
   public void testRejectProjectionOfRepeatedProperty() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select("addresses.postCode")
            .build();
      q.list();
   }

   public void testIsNull1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("surname").isNull()
            .build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testIsNull2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("surname").isNull()
            .build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testIsNull3() {
      if (testNullCollections()) {
         QueryFactory qf = getQueryFactory();

         Query q = qf.from(getModelFactory().getUserImplClass())
               .having("addresses").isNull()
               .build();

         List<User> list = q.list();
         assertEquals(1, list.size());
         assertEquals(3, list.get(0).getId());
      }
   }

   public void testIsNullNumericWithProjection1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .select("name", "surname", "age")
            .orderBy("name", SortOrder.ASC)
            .orderBy("surname", SortOrder.ASC)
            .orderBy("age", SortOrder.ASC)
            .having("age").isNull()
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals("Spider", list.get(0)[0]);
      assertEquals("Man", list.get(0)[1]);
      assertNull(list.get(0)[2]);
      assertEquals("Spider", list.get(1)[0]);
      assertEquals("Woman", list.get(1)[1]);
      assertNull(list.get(1)[2]);
   }

   public void testIsNullNumericWithProjection2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .select("name", "age")
            .not().having("age").isNull()
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0)[0]);
      assertEquals(22, list.get(0)[1]);
   }

   public void testContains1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("accountIds").contains(2)
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testContains2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("accountIds").contains(42)
            .build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testContainsAll1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("accountIds").containsAll(1, 2)
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testContainsAll2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("accountIds").containsAll(Collections.singleton(1))
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testContainsAll3() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("accountIds").containsAll(1, 2, 3)
            .build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testContainsAll4() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("accountIds").containsAll(Collections.emptySet())
            .build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testContainsAny1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("id", SortOrder.ASC)
            .having("accountIds").containsAny(2, 3)
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).getId());
      assertEquals(2, list.get(1).getId());
   }

   public void testContainsAny2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("accountIds").containsAny(4, 5)
            .build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testContainsAny3() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("accountIds").containsAny(Collections.emptySet())
            .build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testIn1() {
      QueryFactory qf = getQueryFactory();

      List<Integer> ids = Arrays.asList(1, 3);
      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("id").in(ids)
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      for (User u : list) {
         assertTrue(ids.contains(u.getId()));
      }
   }

   public void testIn2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("id").in(4)
            .build();

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIn3() {
      QueryFactory qf = getQueryFactory();

      qf.from(getModelFactory().getUserImplClass()).having("id").in(Collections.emptySet());
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIn4() {
      QueryFactory qf = getQueryFactory();

      qf.from(getModelFactory().getUserImplClass()).having("id").in((Collection) null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIn5() {
      QueryFactory qf = getQueryFactory();

      qf.from(getModelFactory().getUserImplClass()).having("id").in((Object[]) null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIn6() {
      QueryFactory qf = getQueryFactory();

      Object[] array = new Object[0];
      qf.from(getModelFactory().getUserImplClass()).having("id").in(array);
   }

   public void testSampleDomainQuery1() {
      QueryFactory qf = getQueryFactory();

      // all male users
      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("name", SortOrder.ASC)
            .having("gender").eq(User.Gender.MALE)
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
   }

   public void testSampleDomainQuery2() {
      QueryFactory qf = getQueryFactory();

      // all male users, but this time retrieved in a twisted manner
      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("name", SortOrder.ASC)
            .not(qf.having("gender").eq(User.Gender.FEMALE))
            .and(qf.not().not(qf.having("gender").eq(User.Gender.MALE)))
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
   }

   public void testStringLiteralEscape() {
      QueryFactory qf = getQueryFactory();

      // all transactions that have a given description. the description contains characters that need to be escaped.
      Query q = qf.from(getModelFactory().getAccountImplClass())
            .having("description").eq("John Doe's first bank account")
            .build();

      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testSortByDate() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getAccountImplClass())
            .orderBy("creationDate", SortOrder.DESC)
            .build();

      List<Account> list = q.list();
      assertEquals(3, list.size());
      assertEquals(3, list.get(0).getId());
      assertEquals(2, list.get(1).getId());
      assertEquals(1, list.get(2).getId());
   }

   public void testSampleDomainQuery3() {
      QueryFactory qf = getQueryFactory();

      // all male users
      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("name", SortOrder.ASC)
            .having("gender").eq(User.Gender.MALE)
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
   }

   public void testSampleDomainQuery4() {
      QueryFactory qf = getQueryFactory();

      // all users ordered descendingly by name
      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("name", SortOrder.DESC)
            .build();

      List<User> list = q.list();
      assertEquals(3, list.size());
      assertEquals("Spider", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
      assertEquals("John", list.get(2).getName());
   }

   public void testSampleDomainQuery4With2SortingOptions() {
      QueryFactory qf = getQueryFactory();

      // all users ordered descendingly by name
      Query q = qf.from(getModelFactory().getUserImplClass())
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

   public void testSampleDomainQuery5() {
      QueryFactory qf = getQueryFactory();

      // name projection of all users ordered descendingly by name
      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("name", SortOrder.DESC)
            .select("name")
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

   public void testSampleDomainQuery6() {
      QueryFactory qf = getQueryFactory();

      // all users with a given name and surname
      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("John")
            .and().having("surname").eq("Doe")
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Doe", list.get(0).getSurname());
   }

   public void testSampleDomainQuery7() {
      QueryFactory qf = getQueryFactory();

      // all rent payments made from a given account
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("accountId").eq(1)
            .and().having("description").like("%rent%")
            .build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
      assertEquals(1, list.get(0).getAccountId());
      assertTrue(list.get(0).getDescription().contains("rent"));
   }

   public void testSampleDomainQuery8() throws Exception {
      QueryFactory qf = getQueryFactory();

      // all the transactions that happened in January 2013
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("date").between(makeDate("2013-01-01"), makeDate("2013-01-31"))
            .build();

      List<Transaction> list = q.list();
      assertEquals(4, list.size());
      for (Transaction t : list) {
         assertTrue(t.getDate().compareTo(makeDate("2013-01-31")) <= 0);
         assertTrue(t.getDate().compareTo(makeDate("2013-01-01")) >= 0);
      }
   }

   public void testSampleDomainQuery9() throws Exception {
      QueryFactory qf = getQueryFactory();

      // all the transactions that happened in January 2013, projected by date field only
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("date")
            .having("date").between(makeDate("2013-01-01"), makeDate("2013-01-31"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(4, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
      assertEquals(1, list.get(2).length);
      assertEquals(1, list.get(3).length);

      for (int i = 0; i < 4; i++) {
         Date d = (Date) list.get(i)[0];
         assertTrue(d.compareTo(makeDate("2013-01-31")) <= 0);
         assertTrue(d.compareTo(makeDate("2013-01-01")) >= 0);
      }
   }

   public void testSampleDomainQuery10() {
      QueryFactory qf = getQueryFactory();

      // all the transactions for a an account having amount greater than a given amount
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("accountId").eq(2)
            .and().having("amount").gt(40)
            .build();

      List<Transaction> list = q.list();
      assertEquals(52, list.size());
      assertTrue(list.get(0).getAmount() > 40);
      assertTrue(list.get(1).getAmount() > 40);
   }

   public void testSampleDomainQuery11() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("John")
            .and().having("addresses.postCode").eq("X1234")
            .and(qf.having("accountIds").eq(1))
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Doe", list.get(0).getSurname());
   }

   public void testSampleDomainQuery12() {
      QueryFactory qf = getQueryFactory();

      // all the transactions that represents credits to the account
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .having("accountId").eq(1)
            .and()
            .not().having("isDebit").eq(true).build();

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertFalse(list.get(0).isDebit());
   }

   public void testSampleDomainQuery13() {
      QueryFactory qf = getQueryFactory();

      // the user that has the bank account with id 3
      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("accountIds").contains(3).build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getId());
      assertTrue(list.get(0).getAccountIds().contains(3));
   }

   public void testSampleDomainQuery14() {
      QueryFactory qf = getQueryFactory();

      // the user that has all the specified bank accounts
      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("accountIds").containsAll(2, 1).build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
      assertTrue(list.get(0).getAccountIds().contains(1));
      assertTrue(list.get(0).getAccountIds().contains(2));
   }

   public void testSampleDomainQuery15() {
      QueryFactory qf = getQueryFactory();

      // the user that has at least one of the specified accounts
      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("accountIds").containsAny(1, 3).build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 2).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 2).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery16() {
      QueryFactory qf = getQueryFactory();

      // third batch of 10 transactions for a given account
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .startOffset(20).maxResults(10)
            .orderBy("id", SortOrder.ASC)
            .having("accountId").eq(2).and().having("description").like("Expensive%")
            .build();

      List<Transaction> list = q.list();
      assertEquals(50, q.getResultSize());
      assertEquals(10, list.size());
      for (int i = 0; i < 10; i++) {
         assertEquals("Expensive shoes " + (20 + i), list.get(i).getDescription());
      }
   }

   public void testSampleDomainQuery17() {
      QueryFactory qf = getQueryFactory();

      // all accounts for a user. first get the user by id and then get his account.
      Query q1 = qf.from(getModelFactory().getUserImplClass())
            .having("id").eq(1).build();

      List<User> users = q1.list();
      Query q2 = qf.from(getModelFactory().getAccountImplClass())
            .orderBy("description", SortOrder.ASC)
            .having("id").in(users.get(0).getAccountIds()).build();

      List<Account> list = q2.list();
      assertEquals(2, list.size());
      assertEquals("John Doe's first bank account", list.get(0).getDescription());
      assertEquals("John Doe's second bank account", list.get(1).getDescription());
   }

   public void testSampleDomainQuery18() {
      QueryFactory qf = getQueryFactory();

      // all transactions of account with id 2 which have an amount larger than 1600 or their description contains the word 'rent'
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .orderBy("description", SortOrder.ASC)
            .having("accountId").eq(1)
            .and(qf.having("amount").gt(1600)
                  .or().having("description").like("%rent%")).build();

      List<Transaction> list = q.list();
      assertEquals(2, list.size());
      assertEquals("Birthday present", list.get(0).getDescription());
      assertEquals("Feb. rent payment", list.get(1).getDescription());
   }

   public void testProjectionOnOptionalField() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .select("id", "age")
            .orderBy("id", SortOrder.ASC)
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(1, list.get(0)[0]);
      assertEquals(2, list.get(1)[0]);
      assertEquals(3, list.get(2)[0]);
      assertEquals(22, list.get(0)[1]);
      assertNull(list.get(1)[1]);
      assertNull(list.get(2)[1]);
   }

   public void testNullOnIntegerField() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("age").isNull()
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertNull(list.get(0).getAge());
      assertNull(list.get(1).getAge());
   }

   public void testIsNotNullOnIntegerField() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("age").isNull()
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Doe", list.get(0).getSurname());
      assertNotNull(list.get(0).getAge());
   }

   public void testSampleDomainQuery19() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("addresses.postCode").in("ZZ", "X1234").build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 2).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 2).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery20() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("addresses.postCode").in("X1234").build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(2, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(2, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery21() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("addresses").isNull().build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 2).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 2).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery22() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("addresses.postCode").like("%123%").build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(2, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(2, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery23() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("id").between(1, 2)
            .build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).getId());
   }

   public void testSampleDomainQuery24() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("id").between(1, 2).includeLower(false)
            .build();

      List<User> list = q.list();

      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery25() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("id").between(1, 2).includeUpper(false)
            .build();

      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(2, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(2, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery26() throws Exception {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getAccountImplClass())
            .having("creationDate").eq(makeDate("2013-01-20"))
            .build();

      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).getId());
   }

   public void testSampleDomainQuery27() throws Exception {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getAccountImplClass())
            .orderBy("id", SortOrder.ASC)
            .having("creationDate").lt(makeDate("2013-01-20"))
            .build();

      List<Account> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).getId());
      assertEquals(2, list.get(1).getId());
   }

   public void testSampleDomainQuery28() throws Exception {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getAccountImplClass())
            .orderBy("id", SortOrder.ASC)
            .having("creationDate").lte(makeDate("2013-01-20"))
            .build();

      List<Account> list = q.list();
      assertEquals(3, list.size());
      assertEquals(1, list.get(0).getId());
      assertEquals(2, list.get(1).getId());
      assertEquals(3, list.get(2).getId());
   }

   public void testSampleDomainQuery29() throws Exception {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getAccountImplClass())
            .having("creationDate").gt(makeDate("2013-01-04"))
            .build();

      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).getId());
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.not().having("name").eq("John").build();
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("John")
            .having("surname").eq("Man")
            .build();
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding3() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not().having("name").eq("John")
            .not().having("surname").eq("Man")
            .build();
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding4() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not(qf.having("name").eq("John"))
            .not(qf.having("surname").eq("Man"))
            .build();
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding5() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .not(qf.having("name").eq("John"))
            .not(qf.having("surname").eq("Man"))
            .build();
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testWrongQueryBuilding6() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("gender").eq(null)
            .build();
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testWrongQueryBuilding7() {
      QueryFactory qf = getQueryFactory();

      FilterConditionEndContext q1 = qf.from(getModelFactory().getUserImplClass())
            .having("gender");

      q1.eq(User.Gender.MALE);
      q1.eq(User.Gender.FEMALE);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014823: maxResults must be greater than 0")
   public void testPagination1() {
      QueryFactory qf = getQueryFactory();

      qf.from(getModelFactory().getUserImplClass())
            .maxResults(0);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014823: maxResults must be greater than 0")
   public void testPagination2() {
      QueryFactory qf = getQueryFactory();

      qf.from(getModelFactory().getUserImplClass())
            .maxResults(-4);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014824: startOffset cannot be less than 0")
   public void testPagination3() {
      QueryFactory qf = getQueryFactory();

      qf.from(getModelFactory().getUserImplClass())
            .startOffset(-3);
   }

   public void testOrderedPagination4() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("id", SortOrder.ASC)
            .maxResults(5)
            .build();

      List<User> list = q.list();
      assertEquals(3, q.getResultSize());
      assertEquals(3, list.size());
   }

   public void testUnorderedPagination4() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .maxResults(5)
            .build();

      List<User> list = q.list();
      assertEquals(3, q.getResultSize());
      assertEquals(3, list.size());
   }

   public void testOrderedPagination5() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("id", SortOrder.ASC)
            .startOffset(20)
            .build();

      List<User> list = q.list();
      assertEquals(3, q.getResultSize());
      assertEquals(0, list.size());
   }

   public void testUnorderedPagination5() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .startOffset(20)
            .build();

      List<User> list = q.list();
      assertEquals(3, q.getResultSize());
      assertEquals(0, list.size());
   }

   public void testOrderedPagination6() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("id", SortOrder.ASC)
            .startOffset(20).maxResults(10)
            .build();

      List<User> list = q.list();
      assertEquals(3, q.getResultSize());
      assertEquals(0, list.size());
   }

   public void testUnorderedPagination6() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .startOffset(20).maxResults(10)
            .build();

      List<User> list = q.list();
      assertEquals(3, q.getResultSize());
      assertEquals(0, list.size());
   }

   public void testOrderedPagination7() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("id", SortOrder.ASC)
            .startOffset(1).maxResults(10)
            .build();

      List<User> list = q.list();
      assertEquals(3, q.getResultSize());
      assertEquals(2, list.size());
   }

   public void testUnorderedPagination7() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .startOffset(1).maxResults(10)
            .build();

      List<User> list = q.list();
      assertEquals(3, q.getResultSize());
      assertEquals(2, list.size());
   }

   public void testOrderedPagination8() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .orderBy("id", SortOrder.ASC)
            .startOffset(0).maxResults(2)
            .build();

      List<User> list = q.list();
      assertEquals(3, q.getResultSize());
      assertEquals(2, list.size());
   }

   public void testUnorderedPagination8() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .startOffset(0).maxResults(2)
            .build();

      List<User> list = q.list();
      assertEquals(3, q.getResultSize());
      assertEquals(2, list.size());
   }

   public void testGroupBy1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .select("name")
            .groupBy("name")
            .orderBy("name")
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("John", list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertEquals("Spider", list.get(1)[0]);
   }

   public void testGroupBy2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(sum("age"))
            .groupBy("name")
            .orderBy("name")
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(22L, list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertEquals(null, list.get(1)[0]);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014026: The expression 'surname' must be part of an aggregate function or it should be included in the GROUP BY clause")
   public void testGroupBy3() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .select("name")
            .groupBy("name")
            .orderBy("surname")
            .build();
      q.list();
   }

   public void testGroupBy4() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(max("addresses.postCode"))
            .groupBy("name")
            .orderBy("name")
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("X1234", list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertEquals("ZZ", list.get(1)[0]);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014021: Queries containing grouping and aggregation functions must use projections.")
   public void testGroupBy5() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .groupBy("name")
            .build();
      q.list();
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Aggregation SUM cannot be applied to property of type java.lang.String")
   public void testGroupBy6() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(sum("name"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(2, list.get(0)[0]);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028515: Cannot have aggregate functions in the WHERE clause : SUM.")
   public void testGroupBy7() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(sum("age"))
            .having(sum("age")).gt(10)
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(3, list.get(0).length);
      assertEquals(3, list.get(1).length);
      assertEquals(1500d, (Double) list.get(0)[2], 0.0001d);
      assertEquals(45d, (Double) list.get(1)[2], 0.0001d);
   }

   public void testHavingWithSum() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(property("accountId"), sum("amount"))
            .groupBy("accountId")
            .having(sum("amount")).gt(3324)
            .orderBy("accountId")
            .build();
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0)[0]);
      assertEquals(6370.0d, (Double) list.get(0)[1], 0.0001d);
   }

   public void testHavingWithAvg() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(property("accountId"), avg("amount"))
            .groupBy("accountId")
            .having(avg("amount")).lt(130.0)
            .orderBy("accountId")
            .build();
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0)[0]);
      assertEquals(120.188679d, (Double) list.get(0)[1], 0.0001d);
   }

   public void testHavingWithMin() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(property("accountId"), min("amount"))
            .groupBy("accountId")
            .having(min("amount")).lt(10)
            .orderBy("accountId")
            .build();
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0)[0]);
      assertEquals(5.0d, (Double) list.get(0)[1], 0.0001d);
   }

   public void testHavingWithMax() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(property("accountId"), max("amount"))
            .groupBy("accountId")
            .having(avg("amount")).lt(150)
            .orderBy("accountId")
            .build();
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0)[0]);
      assertEquals(149.0d, (Double) list.get(0)[1], 0.0001d);
   }

   public void testSum() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(sum("age"))
            .groupBy("name")
            .orderBy("name")
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(22L, list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertEquals(null, list.get(1)[0]);
   }

   public void testEmbeddedSum() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(property("surname"), sum("addresses.number"))
            .groupBy("surname")
            .orderBy("surname")
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(2, list.get(2).length);
      assertEquals(156L, list.get(0)[1]);
      assertEquals(300L, list.get(1)[1]);
      assertNull(list.get(2)[1]);
   }

   public void testGlobalSum() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(sum("amount"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(9693d, (Double) list.get(0)[0], 0.0001d);
   }

   public void testEmbeddedGlobalSum() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(sum("addresses.number"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(456L, list.get(0)[0]);
   }

   public void testCount() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(property("surname"), count("age"))
            .groupBy("surname")
            .orderBy("surname")
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(2, list.get(2).length);
      assertEquals(1L, list.get(0)[1]);
      assertEquals(0L, list.get(1)[1]);
      assertEquals(0L, list.get(2)[1]);
   }

   public void testEmbeddedCount1() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(property("surname"), count("accountIds"))
            .groupBy("surname")
            .orderBy("surname")
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(2, list.get(2).length);
      assertEquals(2L, list.get(0)[1]);
      assertEquals(1L, list.get(1)[1]);
      assertEquals(0L, list.get(2)[1]);
   }

   public void testEmbeddedCount2() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(property("surname"), count("addresses.street"))
            .groupBy("surname")
            .orderBy("surname")
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(2, list.get(2).length);
      assertEquals(1L, list.get(0)[1]);
      assertEquals(2L, list.get(1)[1]);
      assertEquals(0L, list.get(2)[1]);
   }

   public void testGlobalCount() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getAccountImplClass())
            .select(count("creationDate"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(3L, list.get(0)[0]);
   }

   public void testEmbeddedGlobalCount() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(count("accountIds"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(3L, list.get(0)[0]);
   }

   public void testAvg() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(property("accountId"), avg("amount"))
            .groupBy("accountId")
            .orderBy("accountId")
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(1107.6666d, (Double) list.get(0)[1], 0.0001d);
      assertEquals(120.18867d, (Double) list.get(1)[1], 0.0001d);
   }

   public void testEmbeddedAvg() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(property("surname"), avg("addresses.number"))
            .groupBy("surname")
            .orderBy("surname")
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(2, list.get(2).length);
      assertEquals(156d, (Double) list.get(0)[1], 0.0001d);
      assertEquals(150d, (Double) list.get(1)[1], 0.0001d);
      assertEquals(null, list.get(2)[1]);
   }

   public void testGlobalAvg() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(avg("amount"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(173.0892d, (Double) list.get(0)[0], 0.0001d);
   }

   public void testEmbeddedGlobalAvg() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(avg("addresses.number"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(152d, (Double) list.get(0)[0], 0.0001d);
   }

   public void testMin() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(property("accountId"), min("amount"))
            .groupBy("accountId")
            .orderBy("accountId")
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(23d, list.get(0)[1]);
      assertEquals(5d, list.get(1)[1]);
   }

   public void testMinString() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(min("surname"))
            .groupBy("name")
            .orderBy("name")
            .build();
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
      assertEquals("Doe", list.get(0)[0]);
      assertEquals("Man", list.get(1)[0]);
   }

   public void testEmbeddedMin() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(property("surname"), min("addresses.number"))
            .groupBy("surname")
            .orderBy("surname")
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(2, list.get(2).length);
      assertEquals(156, list.get(0)[1]);
      assertEquals(-12, list.get(1)[1]);
      assertEquals(null, list.get(2)[1]);
   }

   public void testGlobalMinDouble() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(min("amount"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(5d, list.get(0)[0]);
   }

   public void testGlobalMinString() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(min("name"))
            .build();
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("John", list.get(0)[0]);
   }

   public void testEmbeddedGlobalMin() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(min("addresses.number"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(-12, list.get(0)[0]);
   }

   public void testMax() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(property("accountId"), max("amount"))
            .groupBy("accountId")
            .orderBy("accountId")
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(1800d, list.get(0)[1]);
      assertEquals(149d, list.get(1)[1]);
   }

   public void testMaxString() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(max("surname"))
            .groupBy("name")
            .orderBy("name")
            .build();
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
      assertEquals("Doe", list.get(0)[0]);
      assertEquals("Woman", list.get(1)[0]);
   }

   public void testEmbeddedMax() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(property("surname"), max("addresses.number"))
            .groupBy("surname")
            .orderBy("surname")
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(2, list.get(2).length);
      assertEquals(156, list.get(0)[1]);
      assertEquals(312, list.get(1)[1]);
      assertEquals(null, list.get(2)[1]);
   }

   public void testEmbeddedMaxString() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(max("addresses.postCode"))
            .groupBy("name")
            .orderBy("name")
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("X1234", list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertEquals("ZZ", list.get(1)[0]);
   }

   public void testGlobalMaxDouble() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(max("amount"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1800d, list.get(0)[0]);
   }

   public void testGlobalMaxString() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(max("name"))
            .build();
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("Spider", list.get(0)[0]);
   }

   public void testEmbeddedGlobalMax() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(max("addresses.number"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(312, list.get(0)[0]);
   }

   public void testOrderBySum() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(sum("age"))
            .orderBy(sum("age"))
            .build();
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(22L, list.get(0)[0]);
   }

   public void testGroupingWithFilter() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select("name")
            .having("name").eq("John")
            .groupBy("name")
            .having("name").eq("John")
            .build();
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("John", list.get(0)[0]);
   }

   public void testCountNull() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(count("age"))
            .build();
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);  // only non-null "age"s were counted
   }

   public void testCountNull2() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(property("name"), count("age"))
            .groupBy("name")
            .orderBy("name")
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals("John", list.get(0)[0]);
      assertEquals(1L, list.get(0)[1]);
      assertEquals(2, list.get(1).length);
      assertEquals("Spider", list.get(1)[0]);
      assertEquals(0L, list.get(1)[1]);
   }

   public void testCountNull3() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(property("name"), count("salutation"))
            .groupBy("name")
            .orderBy("name")
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals("John", list.get(0)[0]);
      assertEquals(0L, list.get(0)[1]);
      assertEquals(2, list.get(1).length);
      assertEquals("Spider", list.get(1)[0]);
      assertEquals(2L, list.get(1)[1]);
   }

   public void testAvgNull() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(avg("age"))
            .build();
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(22.0, list.get(0)[0]);  // only non-null "age"s were used in the average
   }

   public void testDateGrouping1() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("date")
            .having("date").between(makeDate("2013-02-15"), makeDate("2013-03-15"))
            .groupBy("date")
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(makeDate("2013-02-27"), list.get(0)[0]);
   }

   public void testDateGrouping2() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(count("date"), min("date"))
            .having("description").eq("Hotel")
            .groupBy("id")
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);
      assertEquals(makeDate("2013-02-27"), list.get(0)[1]);
   }

   public void testDateGrouping3() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(min("date"), count("date"))
            .having("description").eq("Hotel")
            .groupBy("id")
            .build();

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(makeDate("2013-02-27"), list.get(0)[0]);
      assertEquals(1L, list.get(0)[1]);
   }

   public void testParam() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("gender").eq(param("param2"))
            .build();

      q.setParameter("param2", User.Gender.MALE);

      List<User> list = q.list();

      assertEquals(2, list.size());
      assertEquals(User.Gender.MALE, list.get(0).getGender());
      assertEquals(User.Gender.MALE, list.get(1).getGender());

      q.setParameter("param2", User.Gender.FEMALE);

      list = q.list();

      assertEquals(1, list.size());
      assertEquals(User.Gender.FEMALE, list.get(0).getGender());
   }

   public void testWithParameterMap() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("gender").eq(param("param1"))
            .and()
            .having("name").eq(param("param2"))
            .build();

      Map<String, Object> parameterMap = new HashMap<>(2);
      parameterMap.put("param1", User.Gender.MALE);
      parameterMap.put("param2", "John");

      q.setParameters(parameterMap);

      List<User> list = q.list();

      assertEquals(1, list.size());
      assertEquals(User.Gender.MALE, list.get(0).getGender());
      assertEquals("John", list.get(0).getName());

      parameterMap = new HashMap<>(2);
      parameterMap.put("param1", User.Gender.MALE);
      parameterMap.put("param2", "Spider");

      q.setParameters(parameterMap);

      list = q.list();

      assertEquals(1, list.size());
      assertEquals(User.Gender.MALE, list.get(0).getGender());
      assertEquals("Spider", list.get(0).getName());
   }

   public void testDateParam() throws Exception {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getAccountImplClass())
            .having("creationDate").eq(param("param1"))
            .build().setParameter("param1", makeDate("2013-01-03"));

      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testParamWithGroupBy() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(property("accountId"), property("date"), sum("amount"))
            .groupBy("accountId", "date")
            .having(sum("amount")).gt(param("param"))
            .build();

      q.setParameter("param", 1801);

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).length);
      assertEquals(6225d, list.get(0)[2]);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014805: No parameter named 'param2' was found")
   public void testUnknownParam() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq(param("param1"))
            .build();

      q.setParameter("param2", "John");
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014806: No parameters named '\\[param2\\]' were found")
   public void testUnknownParamWithParameterMap() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq(param("param1"))
            .build();

      Map<String, Object> parameterMap = new HashMap<>(1);
      parameterMap.put("param2", User.Gender.MALE);

      q.setParameters(parameterMap);
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "ISPN014804: Query does not have parameters")
   public void testQueryWithNoParams() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("John")
            .build()
            .setParameter("param1", "John");
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "ISPN014804: Query does not have parameters")
   public void testQueryWithNoParamsWithParameterMap() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("John")
            .build();

      Map<String, Object> parameterMap = new HashMap<>(1);
      parameterMap.put("param1", User.Gender.MALE);

      q.setParameters(parameterMap);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014803: Parameter name cannot be null or empty")
   public void testNullParamName() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq(param(null))
            .build();

      q.setParameter(null, "John");
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014803: Parameter name cannot be null or empty")
   public void testEmptyParamName() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq(param(""))
            .build();

      q.setParameter("", "John");
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "ISPN014825: Query parameter 'param2' was not set")
   public void testMissingParam() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq(param("param1"))
            .and().having("gender").eq(param("param2"))
            .build();

      q.setParameter("param1", "John");

      q.list();
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "ISPN014825: Query parameter 'param2' was not set")
   public void testMissingParamWithParameterMap() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq(param("param1"))
            .and().having("gender").eq(param("param2"))
            .build();

      Map<String, Object> parameterMap = new HashMap<>(1);
      parameterMap.put("param1", "John");

      q.setParameters(parameterMap);

      q.list();
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014812: paramValues cannot be null")
   public void testQueryWithNoParamsWithNullParameterMap() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("name").eq("John")
            .build();

      q.setParameters(null);
   }

   @Test
   public void testComplexQuery() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(avg("amount"), sum("amount"), count("date"), min("date"), max("accountId"))
            .having("isDebit").eq(param("param"))
            .orderBy(avg("amount"), SortOrder.DESC).orderBy(count("date"), SortOrder.DESC)
            .orderBy(max("amount"), SortOrder.ASC)
            .build();

      q.setParameter("param", true);

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(5, list.get(0).length);
      assertEquals(143.50909d, (Double) list.get(0)[0], 0.0001d);
      assertEquals(7893d, (Double) list.get(0)[1], 0.0001d);
      assertEquals(55L, list.get(0)[2]);
      assertEquals(java.util.Date.class, list.get(0)[3].getClass());
      assertTrue(((Date) list.get(0)[3]).compareTo(makeDate("2013-01-01")) == 0);
      assertEquals(2, list.get(0)[4]);
   }

   public void testDateFilteringWithGroupBy() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("date")
            .having("date").between(makeDate("2013-02-15"), makeDate("2013-03-15"))
            .groupBy("date")
            .build();
      List<Object[]> list = q.list();

      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(java.util.Date.class, list.get(0)[0].getClass());
      assertTrue(((Date) list.get(0)[0]).compareTo(makeDate("2013-02-27")) == 0);
   }

   public void testAggregateDate() throws Exception {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(count("date"), min("date"))
            .having("description").eq("Hotel")
            .groupBy("id")
            .build();
      List<Object[]> list = q.list();

      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);
      assertEquals(java.util.Date.class, list.get(0)[1].getClass());
      assertTrue(((Date) list.get(0)[1]).compareTo(makeDate("2013-02-27")) == 0);
   }

   public void testNotIndexedProjection() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("id", "isValid")
            .having("id").gte(98)
            .orderBy("id")
            .build();
      List<Object[]> list = q.list();

      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(98, list.get(0)[0]);
      assertEquals(true, list.get(0)[1]);
      assertEquals(2, list.get(1).length);
      assertEquals(99, list.get(1)[0]);
      assertEquals(true, list.get(1)[1]);
   }

   public void testNotStoredProjection() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("id", "description")
            .having("id").gte(98)
            .orderBy("id")
            .build();
      List<Object[]> list = q.list();

      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(98, list.get(0)[0]);
      assertEquals("Expensive shoes 48", list.get(0)[1]);
      assertEquals(2, list.get(1).length);
      assertEquals(99, list.get(1)[0]);
      assertEquals("Expensive shoes 49", list.get(1)[1]);
   }

   public void testNotIndexedOrderBy() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("id", "isValid")
            .having("id").gte(98)
            .orderBy("isValid")
            .orderBy("id")
            .build();
      List<Object[]> list = q.list();

      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(98, list.get(0)[0]);
      assertEquals(true, list.get(0)[1]);
      assertEquals(2, list.get(1).length);
      assertEquals(99, list.get(1)[0]);
      assertEquals(true, list.get(1)[1]);
   }

   public void testNotStoredOrderBy() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("id", "description")
            .having("id").gte(98)
            .orderBy("description")
            .orderBy("id")
            .build();
      List<Object[]> list = q.list();

      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(98, list.get(0)[0]);
      assertEquals("Expensive shoes 48", list.get(0)[1]);
      assertEquals(2, list.get(1).length);
      assertEquals(99, list.get(1)[0]);
      assertEquals("Expensive shoes 49", list.get(1)[1]);
   }

   public void testDuplicateDateProjection() throws Exception {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("id", "date", "date")
            .having("description").eq("Hotel")
            .build();
      List<Object[]> list = q.list();

      assertEquals(1, list.size());
      assertEquals(3, list.get(0).length);
      assertEquals(3, list.get(0)[0]);
      assertEquals(makeDate("2013-02-27"), list.get(0)[1]);
      assertEquals(makeDate("2013-02-27"), list.get(0)[2]);
   }

   public void testDuplicateBooleanProjection() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select("id", "isDebit", "isDebit")
            .having("description").eq("Hotel")
            .build();
      List<Object[]> list = q.list();

      assertEquals(1, list.size());
      assertEquals(3, list.get(0).length);
      assertEquals(3, list.get(0)[0]);
      assertEquals(true, list.get(0)[1]);
      assertEquals(true, list.get(0)[2]);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014023: Using the multi-valued property path 'addresses.street' in the GROUP BY clause is not currently supported")
   public void testGroupByMustNotAcceptRepeatedProperty() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(min("name"))
            .groupBy("addresses.street")
            .build();
      q.list();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014024: The property path 'addresses.street' cannot be used in the ORDER BY clause because it is multi-valued")
   public void testOrderByMustNotAcceptRepeatedProperty() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select("name")
            .orderBy("addresses.street")
            .build();
      q.list();
   }

   public void testOrderByInAggregationQueryMustAcceptRepeatedProperty() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(avg("age"), property("name"))
            .having("name").gt("A")
            .groupBy("name")
            .having(max("addresses.street")).gt("A")
            .orderBy(min("addresses.street"))
            .build();

      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertNull(list.get(0)[0]);
      assertEquals("Spider", list.get(0)[1]);
      assertEquals(22.0, list.get(1)[0]);
      assertEquals("John", list.get(1)[1]);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028515: Cannot have aggregate functions in the WHERE clause : MIN.")
   public void testRejectAggregationsInWhereClause() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select("name")
            .having("name").eq(min("addresses.street"))
            .build();
      q.list();
   }

   public void testAggregateRepeatedField() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(min("addresses.street"))
            .having("name").eq("Spider")
            .build();

      List<Object[]> list = q.list();
      assertEquals("Bond Street", list.get(0)[0]);
   }

   public void testGroupingAndAggregationOnSameField() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(count("surname"))
            .groupBy("surname")
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);
      assertEquals(1L, list.get(1)[0]);
      assertEquals(1L, list.get(2)[0]);
   }

   public void testTwoPhaseGroupingAndAggregationOnSameField() {
      QueryFactory qf = getQueryFactory();
      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(count("surname"), sum("addresses.number"))
            .groupBy("surname")
            .orderBy("surname")
            .build();

      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);
      assertEquals(156L, list.get(0)[1]);
      assertEquals(1L, list.get(1)[0]);
      assertEquals(300L, list.get(1)[1]);
      assertEquals(1L, list.get(2)[0]);
      assertNull(list.get(2)[1]);
   }

   /**
    * Test that 'like' accepts only % and _ as wildcards.
    */
   public void testLuceneWildcardsAreEscaped() {
      QueryFactory qf = getQueryFactory();

      // use a true wildcard
      Query q1 = qf.from(getModelFactory().getUserImplClass())
            .having("name").like("J%n")
            .build();
      assertEquals(1, q1.list().size());

      // use an improper wildcard
      Query q2 = qf.from(getModelFactory().getUserImplClass())
            .having("name").like("J*n")
            .build();
      assertEquals(0, q2.list().size());

      // use a true wildcard
      Query q3 = qf.from(getModelFactory().getUserImplClass())
            .having("name").like("Jo_n")
            .build();
      assertEquals(1, q3.list().size());

      // use an improper wildcard
      Query q4 = qf.from(getModelFactory().getUserImplClass())
            .having("name").like("Jo?n")
            .build();
      assertEquals(0, q4.list().size());
   }

   public void testCompareLongWithInt() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .select(sum("age"))
            .groupBy("name")
            .having(sum("age")).gt(50000)
            .build();

      List<Object[]> list = q.list();
      assertEquals(0, list.size());
   }

   public void testCompareDoubleWithInt() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getTransactionImplClass())
            .select(sum("amount"))
            .groupBy("accountId")
            .having(sum("amount")).gt(50000)
            .build();

      List<Object[]> list = q.list();
      assertEquals(0, list.size());
   }

   public void testFullTextTerm() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.create("from " + getModelFactory().getTransactionTypeName() + " where longDescription:'rent'");

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
   }

   public void testFullTextPhrase() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.create("from " + getModelFactory().getTransactionTypeName() + " where longDescription:'expensive shoes'");

      List<Transaction> list = q.list();
      assertEquals(50, list.size());
   }

   public void testInstant1() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("creationDate").eq(Instant.parse("2011-12-03T10:15:30Z"))
            .build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testInstant2() {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("passwordExpirationDate").eq(Instant.parse("2011-12-03T10:15:30Z"))
            .build();

      List<User> list = q.list();
      assertEquals(3, list.size());
   }
}

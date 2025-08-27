package org.infinispan.query.dsl.embedded;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.objectfilter.ParsingException;
import org.infinispan.test.TestingUtil;
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
@Test(groups = {"functional", "smoke"}, testName = "query.dsl.embedded.QueryConditionsTest")
public class QueryConditionsTest extends AbstractQueryTest {
   protected final String ACCOUNT_TYPE = getModelFactory().getAccountTypeName();
   protected final String TRANSACTION_TYPE = getModelFactory().getTransactionTypeName();
   protected final String USER_TYPE = getModelFactory().getUserTypeName();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.transaction().transactionMode(TransactionMode.TRANSACTIONAL).indexing().enable().storage(LOCAL_HEAP).addIndexedEntity(getModelFactory().getUserImplClass()).addIndexedEntity(getModelFactory().getAccountImplClass()).addIndexedEntity(getModelFactory().getTransactionImplClass());
      createClusteredCaches(1, DslSCI.INSTANCE, cfg);
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
      SearchMapping searchMapping = TestingUtil.extractComponent((Cache<?, ?>) getCacheForQuery(), SearchMapping.class);

      verifyClassIsIndexed(searchMapping, getModelFactory().getUserImplClass());
      verifyClassIsIndexed(searchMapping, getModelFactory().getAccountImplClass());
      verifyClassIsIndexed(searchMapping, getModelFactory().getTransactionImplClass());
      verifyClassIsNotIndexed(searchMapping, getModelFactory().getAddressImplClass());
   }

   private void verifyClassIsNotIndexed(SearchMapping searchMapping, Class<?> type) {
      assertNull(searchMapping.indexedEntity(type));
   }

   private void verifyClassIsIndexed(SearchMapping searchMapping, Class<?> type) {
      assertNotNull(searchMapping.indexedEntity(type));
   }

   public void testEq1() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name='John'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Doe", list.get(0).getSurname());
   }

   public void testEqEmptyString() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name=''");
      List<User> list = q.list();
      assertTrue(list.isEmpty());
   }

   public void testEqSentence() {
      Query<Account> q = queryCache("FROM " + ACCOUNT_TYPE + " WHERE description='John Doe''s first bank account'");
      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testEq() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name='Jacob'");

      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testEqNonIndexedField() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE notes='Lorem ipsum dolor sit amet'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testEqHybridQuery() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE notes='Lorem ipsum dolor sit amet' AND surname='Doe'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testEqHybridQueryWithParam() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE notes='Lorem ipsum dolor sit amet' AND surname=:surnameParam");
      q.setParameter("surnameParam", "Doe");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testEqHybridQueryWithPredicateOptimisation() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE notes like '%ipsum%' AND (name='John' OR name='Jane')");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Lorem ipsum dolor sit amet", list.get(0).getNotes());
   }

   public void testEqInNested1() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " u WHERE u.addresses.postCode='X1234'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("X1234", list.get(0).getAddresses().get(0).getPostCode());
   }

   public void testEqInNested2() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " u WHERE u.addresses.postCode='Y12'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getAddresses().size());
   }

   public void testLike() {
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE description like '%rent%'");
      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getAccountId());
      assertEquals(1500, list.get(0).getAmount(), 0);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "Input is not a valid date string: ")
   public void testBetweenArgsAreComparable() {
      queryCache("FROM " + TRANSACTION_TYPE + " WHERE date between '' AND ''").execute();
   }

   public void testBetween1() throws Exception {
      Query<Transaction> q = queryCache(
            "FROM %s WHERE date BETWEEN '%s' AND '%s'",
            TRANSACTION_TYPE,
            queryDate(LocalDate.of(2013, 1, 1)),
            queryDate(LocalDate.of(2013, 1, 31))
      );
      List<Transaction> list = q.list();
      assertEquals(4, list.size());
      for (Transaction t : list) {
         assertTrue(compareDate(t.getDate(), LocalDate.of(2013, 1, 31)) <= 0);
         assertTrue(compareDate(t.getDate(), LocalDate.of(2013, 1, 1)) >= 0);
      }
   }

   public void testGt() {
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE amount > 1500");
      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertTrue(list.get(0).getAmount() > 1500);
   }

   public void testGte() {
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE amount >= 1500");
      List<Transaction> list = q.list();
      assertEquals(2, list.size());
      for (Transaction t : list) {
         assertTrue(t.getAmount() >= 1500);
      }
   }

   public void testLt() {
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE amount < 1500");
      List<Transaction> list = q.list();
      assertEquals(54, list.size());
      for (Transaction t : list) {
         assertTrue(t.getAmount() < 1500);
      }
   }

   public void testLte() {
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE amount <= 1500");
      List<Transaction> list = q.list();
      assertEquals(55, list.size());
      for (Transaction t : list) {
         assertTrue(t.getAmount() <= 1500);
      }
   }

   // This tests against https://hibernate.atlassian.net/browse/HSEARCH-2030
   public void testLteOnFieldWithNullToken() {
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE description <= '-Popcorn'");
      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertEquals("-Popcorn", list.get(0).getDescription());
   }

   public void testAnd1() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name='Spider' AND surname='Man'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getId());
   }

   public void testAnd4() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name='Spider' OR name='John' and surname='Man'");
      List<User> list = q.list();
      assertEquals(2, list.size());
   }

   public void testOr1() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE surname='Man' OR surname='Woman'");
      List<User> list = q.list();
      assertEquals(2, list.size());
      for (User u : list) {
         assertEquals("Spider", u.getName());
      }
   }

   public void testOr4() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE ((gender = 'MALE' OR name = 'Spider') AND gender = 'FEMALE') OR surname LIKE '%oe%' ORDER BY surname DESC");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals("Woman", list.get(0).getSurname());
      assertEquals("Doe", list.get(1).getSurname());
   }

   public void testOr5() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE (gender = 'MALE' OR name = 'Spider' OR gender = 'FEMALE') AND surname LIKE '%oe%'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
   }

   public void testNot1() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name != 'Spider'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
   }

   public void testNot3() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name != 'John' AND surname = 'Man'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Spider", list.get(0).getName());
   }

   public void testNot4() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE surname = 'Man' AND name != 'John'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Spider", list.get(0).getName());
   }

   public void testNot5() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name != 'Spider' OR surname = 'Man'");
      List<User> list = q.list();
      assertEquals(2, list.size());
      for (User u : list) {
         assertNotEquals(u.getSurname(), "Woman");
      }
   }

   public void testNot6() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE NOT NOT gender = 'FEMALE'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Woman", list.get(0).getSurname());
   }

   public void testNot7() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE gender = 'FEMALE' AND NOT name = 'Spider'");
      List<User> list = q.list();
      assertTrue(list.isEmpty());
   }

   public void testNot8() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE NOT (name = 'John' OR surname = 'Man')");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Spider", list.get(0).getName());
      assertEquals("Woman", list.get(0).getSurname());
   }

   public void testNot9() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE NOT (name = 'John' AND surname = 'Doe') ORDER BY id ASC");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals("Spider", list.get(0).getName());
      assertEquals("Man", list.get(0).getSurname());
      assertEquals("Spider", list.get(1).getName());
      assertEquals("Woman", list.get(1).getSurname());
   }

   public void testNot10() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name = 'John' OR surname = 'Man'");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertNotEquals(list.get(0).getSurname(), "Woman");
   }

   public void testNot11() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE NOT NOT (name = 'John' OR surname = 'Man')");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertNotEquals(list.get(0).getSurname(), "Woman");
   }

   public void testEmptyQuery() {
      Query<User> q = queryCache("FROM " + USER_TYPE);
      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testTautology() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name > 'A' OR name <= 'A'");
      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testContradiction() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name > 'A' AND name <= 'A'");
      List<User> list = q.list();
      assertTrue(list.isEmpty());
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028503:.*")
   public void testInvalidEmbeddedAttributeQuery() {
      Query<User> q = queryCache("SELECT addresses FROM " + USER_TYPE);
      q.list();  // exception thrown only at execution time when in remote mode!
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014027: The property path 'addresses.postCode' cannot be projected because it is multi-valued")
   public void testRejectProjectionOfRepeatedProperty() {
      Query<User> q = queryCache("SELECT u.addresses.postCode FROM " + USER_TYPE + " u");
      q.list();  // exception thrown only at execution time
   }

   public void testIsNull1() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE surname IS NULL");
      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   public void testIsNull2() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE surname IS NOT NULL");
      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testIsNull3() {
      if (testNullCollections()) {
         Query<User> q = queryCache("FROM " + USER_TYPE + " u WHERE u.addresses IS NULL");
         List<User> list = q.list();
         assertEquals(1, list.size());
         assertEquals(3, list.get(0).getId());
      }
   }

   public void testIsNullNumericWithProjection1() {
      Query<Object[]> q = queryCache("SELECT name, surname, age FROM " + USER_TYPE + " WHERE age IS NULL OR age = -1 ORDER BY name ASC, surname ASC, age ASC");
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
      Query<Object[]> q = queryCache("SELECT name, age FROM " + USER_TYPE + " WHERE age IS NOT NULL AND age != -1");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0)[0]);
      assertEquals(22, list.get(0)[1]);
   }

   public void testIn1() {
      List<Integer> ids = Arrays.asList(1, 3);
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE id IN (1, 3)");
      List<User> list = q.list();
      assertEquals(2, list.size());
      for (User u : list) {
         assertTrue(ids.contains(u.getId()));
      }
   }

   public void testIn2() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE id IN (4)");
      List<User> list = q.list();
      assertEquals(0, list.size());
   }

   @Test(expectedExceptions = ParsingException.class)
   public void testIn3() {
      queryCache("FROM " + USER_TYPE + " WHERE id IN ()").execute();
   }

   public void testSampleDomainQuery1() {      // all male users
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE gender = 'MALE' ORDER BY name ASC");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
   }

   public void testSampleDomainQuery2() {      // all male users, but this time retrieved in a twisted manner
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE NOT gender = 'FEMALE' AND gender = 'MALE' ORDER BY name ASC");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
   }

   public void testStringLiteralEscape() {
      // all transactions that have a given description. the description contains characters that need to be escaped.
      Query<Account> q = queryCache("FROM " + ACCOUNT_TYPE + " WHERE description = 'John Doe''s first bank account'");
      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testSortByDate() {
      Query<Account> q = queryCache("FROM " + ACCOUNT_TYPE + " ORDER BY creationDate DESC");
      List<Account> list = q.list();
      assertEquals(3, list.size());
      assertEquals(3, list.get(0).getId());
      assertEquals(2, list.get(1).getId());
      assertEquals(1, list.get(2).getId());
   }

   public void testSampleDomainQuery4With2SortingOptions() {      // all users ordered descendingly by name
      Query<User> q = queryCache("FROM " + USER_TYPE + " ORDER BY name DESC, surname ASC");
      List<User> list = q.list();
      assertEquals(3, list.size());
      assertEquals("Spider", list.get(0).getName());
      assertEquals("Spider", list.get(1).getName());
      assertEquals("John", list.get(2).getName());
      assertEquals("Man", list.get(0).getSurname());
      assertEquals("Woman", list.get(1).getSurname());
      assertEquals("Doe", list.get(2).getSurname());
   }

   public void testSampleDomainQuery5() {      // name projection of all users ordered descendingly by name
      Query<Object[]> q = queryCache("SELECT name FROM " + USER_TYPE + " ORDER BY name DESC");
      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
      assertEquals(1, list.get(2).length);
      assertEquals("Spider", list.get(0)[0]);
      assertEquals("Spider", list.get(1)[0]);
      assertEquals("John", list.get(2)[0]);
   }

   public void testSampleDomainQuery6() {      // all users with a given name and surname
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name = 'John' AND surname = 'Doe'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Doe", list.get(0).getSurname());
   }

   public void testSampleDomainQuery7() {      // all rent payments made from a given account
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE accountId = 1 AND description LIKE '%rent%'");
      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
      assertEquals(1, list.get(0).getAccountId());
      assertTrue(list.get(0).getDescription().contains("rent"));
   }

   public void testSampleDomainQuery8() throws Exception {      // all the transactions that happened in January 2013
      Query<Transaction> q = queryCache("FROM %s WHERE date BETWEEN '%s' AND '%s'",
            TRANSACTION_TYPE,
            queryDate(LocalDate.of(2013, 1, 1)),
            queryDate(LocalDate.of(2013, 1, 31))
      );
      List<Transaction> list = q.list();
      assertEquals(4, list.size());
      for (Transaction t : list) {
         assertTrue(compareDate(t.getDate(), LocalDate.of(2013, 1, 31)) <= 0);
         assertTrue(compareDate(t.getDate(), LocalDate.of(2013, 1, 1)) >= 0);
      }
   }

   public void testSampleDomainQuery9() throws Exception {      // all the transactions that happened in January 2013, projected by date field only
      Query<Object[]> q = queryCache(
            "SELECT date FROM %s WHERE date BETWEEN '%s' AND '%s'",
            TRANSACTION_TYPE,
            queryDate(LocalDate.of(2013, 1, 1)),
            queryDate(LocalDate.of(2013, 1, 31))
      );
      List<Object[]> list = q.list();
      assertEquals(4, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
      assertEquals(1, list.get(2).length);
      assertEquals(1, list.get(3).length);

      for (int i = 0; i < 4; i++) {
         Date d = (Date) list.get(i)[0];
         assertTrue(compareDate(d, LocalDate.of(2013, 1, 31)) <= 0);
         assertTrue(compareDate(d, LocalDate.of(2013, 1, 1)) >= 0);
      }
   }

   public void testSampleDomainQuery10() {      // all the transactions for an account having amount greater than a given amount
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE accountId = 2 AND amount > 40");
      List<Transaction> list = q.list();
      assertEquals(52, list.size());
      assertTrue(list.get(0).getAmount() > 40);
      assertTrue(list.get(1).getAmount() > 40);
   }

   public void testSampleDomainQuery11() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " u WHERE u.name = 'John' AND u.addresses.postCode = 'X1234' AND u.accountIds = 1");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("Doe", list.get(0).getSurname());
   }

   public void testSampleDomainQuery12() {      // all the transactions that represents credits to the account
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE accountId = 1 AND isDebit != true");
      List<Transaction> list = q.list();
      assertEquals(1, list.size());
      assertFalse(list.get(0).isDebit());
   }

   public void testSampleDomainQuery13() {      // the user that has the bank account with id 3
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE accountIds = 3");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getId());
      assertTrue(list.get(0).getAccountIds().contains(3));
   }

   public void testSampleDomainQuery14() {      // the user that has all the specified bank accounts
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE accountIds = 2 AND accountIds = 1");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
      assertTrue(list.get(0).getAccountIds().contains(1));
      assertTrue(list.get(0).getAccountIds().contains(2));
   }

   public void testSampleDomainQuery15() {      // the user that has at least one of the specified accounts
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE accountIds = 1 OR accountIds = 3");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 2).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 2).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery16() {      // third batch of 10 transactions for a given account
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE accountId = 2 AND description LIKE 'Expensive%' ORDER BY id ASC");
      List<Transaction> list = q.startOffset(20).maxResults(10).list();
      assertEquals(10, list.size());
      for (int i = 0; i < 10; i++) {
         assertEquals("Expensive shoes " + (20 + i), list.get(i).getDescription());
      }
   }

   public void testSampleDomainQuery17() {      // all accounts for a user. first get the user by id and then get his account.
      Query<User> q1 = queryCache("FROM " + USER_TYPE + " WHERE id = 1");
      List<User> users = q1.list();
      Query<Account> q2 = queryCache("FROM " + ACCOUNT_TYPE + " WHERE id IN (1, 2) ORDER BY description ASC");
      List<Account> list = q2.list();
      assertEquals(2, list.size());
      assertEquals("John Doe's first bank account", list.get(0).getDescription());
      assertEquals("John Doe's second bank account", list.get(1).getDescription());
   }

   public void testSampleDomainQuery18() {      // all transactions of account with id 2 which have an amount larger than 1600 OR their description contains the word 'rent'
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE accountId = 1 AND (amount > 1600 OR description LIKE '%rent%') ORDER BY description ASC");
      List<Transaction> list = q.list();
      assertEquals(2, list.size());
      assertEquals("Birthday present", list.get(0).getDescription());
      assertEquals("Feb. rent payment", list.get(1).getDescription());
   }

   public void testProjectionOnOptionalField() {
      Query<Object[]> q = queryCache("SELECT id, age FROM " + USER_TYPE + " ORDER BY id ASC");
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
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE age IS NULL OR age = -1");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertNull(list.get(0).getAge());
      assertNull(list.get(1).getAge());
   }

   public void testIsNotNullOnIntegerField() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE age IS NOT NULL AND age != -1");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals("John", list.get(0).getName());
      assertEquals("Doe", list.get(0).getSurname());
      assertNotNull(list.get(0).getAge());
   }

   public void testSampleDomainQuery19() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " u WHERE u.addresses.postCode IN ('ZZ', 'X1234')");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 2).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 2).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery20() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " u WHERE u.addresses.postCode NOT IN ('X1234')");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(2, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(2, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery21() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " u WHERE u.addresses IS NOT NULL");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 2).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 2).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery22() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " u WHERE u.addresses.postCode NOT LIKE '%123%'");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(2, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(2, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery23() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE NOT id BETWEEN 1 AND 2");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).getId());
   }

   public void testSampleDomainQuery24() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE (id <= 1 OR id > 2)");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(1, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(1, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery25() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE (id < 1 OR id >= 2)");
      List<User> list = q.list();
      assertEquals(2, list.size());
      assertTrue(Arrays.asList(2, 3).contains(list.get(0).getId()));
      assertTrue(Arrays.asList(2, 3).contains(list.get(1).getId()));
   }

   public void testSampleDomainQuery26() {
      Query<Account> q = queryCache("FROM %s WHERE creationDate = '%s'",
            ACCOUNT_TYPE,
            queryDate(LocalDate.of(2013, 1, 20))
      );
      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).getId());
   }

   public void testSampleDomainQuery27() {
      Query<Account> q = queryCache("FROM %s WHERE creationDate < '%s' ORDER BY id ASC",
            ACCOUNT_TYPE,
            queryDate(LocalDate.of(2013, 1, 20))
      );
      List<Account> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).getId());
      assertEquals(2, list.get(1).getId());
   }

   public void testSampleDomainQuery28() {
      Query<Account> q = queryCache("FROM %s WHERE creationDate <= '%s' ORDER BY id ASC",
            ACCOUNT_TYPE,
            queryDate(LocalDate.of(2013, 1, 20))
      );
      List<Account> list = q.list();
      assertEquals(3, list.size());
      assertEquals(1, list.get(0).getId());
      assertEquals(2, list.get(1).getId());
      assertEquals(3, list.get(2).getId());
   }

   public void testSampleDomainQuery29() {
      Query<Account> q = queryCache("FROM %s WHERE creationDate > '%s'",
            ACCOUNT_TYPE,
            queryDate(LocalDate.of(2013, 1, 4))
      );
      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).getId());
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014823: maxResults cannot be less than 0")
   public void testPagination1() {
      queryCache("FROM " + USER_TYPE).maxResults(-1).execute();
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014824: startOffset cannot be less than 0")
   public void testPagination2() {
      queryCache("FROM " + USER_TYPE).startOffset(-3).execute();
   }

   public void testOrderedPagination4() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " ORDER BY id ASC");
      q.maxResults(5);
      QueryResult<User> r = q.execute();
      List<User> list = r.list();
      assertEquals(3, r.count().value());
      assertEquals(3, list.size());
   }

   public void testUnorderedPagination4() {
      Query<User> q = queryCache("FROM " + USER_TYPE);
      q.maxResults(5);
      QueryResult<User> r = q.execute();
      List<User> list = r.list();
      assertEquals(3, r.count().value());
      assertEquals(3, list.size());
   }

   public void testOrderedPagination5() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " ORDER BY id ASC");
      q.startOffset(20);
      QueryResult<User> r = q.execute();
      List<User> list = r.list();
      assertEquals(3, r.count().value());
      assertEquals(0, list.size());
   }

   public void testUnorderedPagination5() {
      Query<User> q = queryCache("FROM " + USER_TYPE);
      q.startOffset(20);
      QueryResult<User> r = q.execute();
      List<User> list = r.list();
      assertEquals(3, r.count().value());
      assertEquals(0, list.size());
   }

   public void testOrderedPagination6() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " ORDER BY id ASC");
      q.startOffset(20).maxResults(10);
      QueryResult<User> r = q.execute();
      List<User> list = r.list();
      assertEquals(3, r.count().value());
      assertEquals(0, list.size());
   }

   public void testUnorderedPagination6() {
      Query<User> q = queryCache("FROM " + USER_TYPE);
      q.startOffset(20).maxResults(10);
      QueryResult<User> r = q.execute();
      List<User> list = r.list();
      assertEquals(3, r.count().value());
      assertEquals(0, list.size());
   }

   public void testOrderedPagination7() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " ORDER BY id ASC");
      q.startOffset(1).maxResults(10);
      QueryResult<User> r = q.execute();
      List<User> list = r.list();
      assertEquals(3, r.count().value());
      assertEquals(2, list.size());
   }

   public void testUnorderedPagination7() {
      Query<User> q = queryCache("FROM " + USER_TYPE);
      q.startOffset(1).maxResults(10);
      QueryResult<User> r = q.execute();
      List<User> list = r.list();
      assertEquals(3, r.count().value());
      assertEquals(2, list.size());
   }

   public void testOrderedPagination8() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " ORDER BY id ASC");
      q.startOffset(0).maxResults(2);
      QueryResult<User> r = q.execute();
      List<User> list = r.list();
      assertEquals(3, r.count().value());
      assertEquals(2, list.size());
   }

   public void testUnorderedPagination8() {
      Query<User> q = queryCache("FROM " + USER_TYPE);
      q.startOffset(0).maxResults(2);
      QueryResult<User> r = q.execute();
      assertEquals(3, r.count().value());
      List<User> list = r.list();
      assertEquals(2, list.size());
   }

   public void testGroupBy1() {
      Query<Object[]> q = queryCache("SELECT name FROM " + USER_TYPE + " GROUP BY name ORDER BY name ASC");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("John", list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertEquals("Spider", list.get(1)[0]);
   }

   public void testGroupBy2() {
      Query<Object[]> q = queryCache("SELECT SUM(age) FROM " + USER_TYPE + " GROUP BY name ORDER BY name ASC");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(22L, list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertNull(list.get(1)[0]);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014026: The expression 'surname' must be part of an aggregate function or it should be included in the GROUP BY clause")
   public void testGroupBy3() {
      Query<Object[]> q = queryCache("SELECT name FROM " + USER_TYPE + " GROUP BY name ORDER BY surname ASC");
      q.list();
   }

   public void testGroupBy4() {
      Query<Object[]> q = queryCache("SELECT MAX(u.addresses.postCode) FROM " + USER_TYPE + " u GROUP BY u.name ORDER BY u.name ASC");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("X1234", list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertEquals("ZZ", list.get(1)[0]);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014021: Queries containing grouping and aggregation functions must use projections.")
   public void testGroupBy5() {
      Query<Object[]> q = queryCache("FROM " + USER_TYPE + " GROUP BY name ");
      q.list();
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Aggregation SUM cannot be applied to property of type java.lang.String")
   public void testGroupBy6() {
      Query<Object[]> q = queryCache("SELECT SUM(name) FROM " + USER_TYPE);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(2, list.get(0)[0]);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028515: Cannot have aggregate functions in the WHERE clause : SUM.")
   public void testGroupBy7() {
      Query<Object[]> q = queryCache("SELECT SUM(age) FROM " + USER_TYPE + " WHERE SUM(age) > 10");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(3, list.get(0).length);
      assertEquals(3, list.get(1).length);
      assertEquals(1500d, (Double) list.get(0)[2], 0.0001d);
      assertEquals(45d, (Double) list.get(1)[2], 0.0001d);
   }

   public void testHavingWithSum() {
      Query<Object[]> q = queryCache("SELECT accountId, SUM(amount) FROM " + TRANSACTION_TYPE + " GROUP BY accountId HAVING SUM(amount) > 3324 ORDER BY accountId ASC");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0)[0]);
      assertEquals(6370.0d, (Double) list.get(0)[1], 0.0001d);
   }

   public void testHavingWithAvg() {
      Query<Object[]> q = queryCache("SELECT accountId, AVG(amount) FROM " + TRANSACTION_TYPE + " GROUP BY accountId HAVING AVG(amount) < 130.0 ORDER BY accountId ASC");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0)[0]);
      assertEquals(120.188679d, (Double) list.get(0)[1], 0.0001d);
   }

   public void testHavingWithMin() {
      Query<Object[]> q = queryCache("SELECT accountId, MIN(amount) FROM " + TRANSACTION_TYPE + " GROUP BY accountId HAVING MIN(amount) < 10 ORDER BY accountId ASC");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0)[0]);
      assertEquals(5.0d, (Double) list.get(0)[1], 0.0001d);
   }

   public void testHavingWithMax() {
      Query<Object[]> q = queryCache("SELECT accountId, MAX(amount) FROM " + TRANSACTION_TYPE + " GROUP BY accountId HAVING AVG(amount) < 150 ORDER BY accountId ASC");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0)[0]);
      assertEquals(149.0d, (Double) list.get(0)[1], 0.0001d);
   }

   public void testSum() {
      Query<Object[]> q = queryCache("SELECT SUM(age) FROM " + USER_TYPE + " GROUP BY name ORDER BY name ASC");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(22L, list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertNull(list.get(1)[0]);
   }

   public void testEmbeddedSum() {
      Query<Object[]> q = queryCache("SELECT u.surname, SUM(u.addresses.number) FROM " + USER_TYPE + " u GROUP BY u.surname ORDER BY u.surname ASC");
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
      Query<Object[]> q = queryCache("SELECT SUM(amount) FROM " + TRANSACTION_TYPE);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(9693d, (Double) list.get(0)[0], 0.0001d);
   }

   public void testEmbeddedGlobalSum() {
      Query<Object[]> q = queryCache("SELECT SUM(u.addresses.number) FROM " + USER_TYPE + " u");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(456L, list.get(0)[0]);
   }

   public void testCount() {
      Query<Object[]> q = queryCache("SELECT surname, COUNT(age) FROM " + USER_TYPE + " GROUP BY surname ORDER BY surname ASC");
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
      Query<Object[]> q = queryCache("SELECT surname, COUNT(accountIds) FROM " + USER_TYPE + " GROUP BY surname ORDER BY surname ASC");
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
      Query<Object[]> q = queryCache("SELECT u.surname, COUNT(u.addresses.street) FROM " + USER_TYPE + " u GROUP BY u.surname ORDER BY u.surname ASC");
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
      Query<Object[]> q = queryCache("SELECT COUNT(creationDate) FROM " + ACCOUNT_TYPE);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(3L, list.get(0)[0]);
   }

   public void testEmbeddedGlobalCount() {
      Query<Object[]> q = queryCache("SELECT COUNT(accountIds) FROM " + USER_TYPE);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(3L, list.get(0)[0]);
   }

   public void testAvg() {
      Query<Object[]> q = queryCache("SELECT accountId, AVG(amount) FROM " + TRANSACTION_TYPE + " GROUP BY accountId ORDER BY accountId ASC");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(1107.6666d, (Double) list.get(0)[1], 0.0001d);
      assertEquals(120.18867d, (Double) list.get(1)[1], 0.0001d);
   }

   public void testEmbeddedAvg() {
      Query<Object[]> q = queryCache("SELECT u.surname, AVG(u.addresses.number) FROM " + USER_TYPE + " u GROUP BY u.surname ORDER BY u.surname ASC");
      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(2, list.get(2).length);
      assertEquals(156d, (Double) list.get(0)[1], 0.0001d);
      assertEquals(150d, (Double) list.get(1)[1], 0.0001d);
      assertNull(list.get(2)[1]);
   }

   public void testGlobalAvg() {
      Query<Object[]> q = queryCache("SELECT AVG(amount) FROM " + TRANSACTION_TYPE);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(173.0892d, (Double) list.get(0)[0], 0.0001d);
   }

   public void testEmbeddedGlobalAvg() {
      Query<Object[]> q = queryCache("SELECT AVG(u.addresses.number) FROM " + USER_TYPE + " u");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(152d, (Double) list.get(0)[0], 0.0001d);
   }

   public void testMin() {
      Query<Object[]> q = queryCache("SELECT accountId, MIN(amount) FROM " + TRANSACTION_TYPE + " GROUP BY accountId ORDER BY accountId ASC");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(23d, list.get(0)[1]);
      assertEquals(5d, list.get(1)[1]);
   }

   public void testMinString() {
      Query<Object[]> q = queryCache("SELECT MIN(surname) FROM " + USER_TYPE + " GROUP BY name ORDER BY name ASC");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
      assertEquals("Doe", list.get(0)[0]);
      assertEquals("Man", list.get(1)[0]);
   }

   public void testEmbeddedMin() {
      Query<Object[]> q = queryCache("SELECT u.surname, MIN(u.addresses.number) FROM " + USER_TYPE + " u GROUP BY u.surname ORDER BY u.surname ASC");
      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(2, list.get(2).length);
      assertEquals(156, list.get(0)[1]);
      assertEquals(-12, list.get(1)[1]);
      assertNull(list.get(2)[1]);
   }

   public void testGlobalMinDouble() {
      Query<Object[]> q = queryCache("SELECT MIN(amount) FROM " + TRANSACTION_TYPE);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(5d, list.get(0)[0]);
   }

   public void testGlobalMinString() {
      Query<Object[]> q = queryCache("SELECT MIN(name) FROM " + USER_TYPE);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("John", list.get(0)[0]);
   }

   public void testEmbeddedGlobalMin() {
      Query<Object[]> q = queryCache("SELECT MIN(u.addresses.number) FROM " + USER_TYPE + " u");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(-12, list.get(0)[0]);
   }

   public void testMax() {
      Query<Object[]> q = queryCache("SELECT accountId, MAX(amount) FROM " + TRANSACTION_TYPE + " GROUP BY accountId ORDER BY accountId ASC");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(1800d, list.get(0)[1]);
      assertEquals(149d, list.get(1)[1]);
   }

   public void testMaxString() {
      Query<Object[]> q = queryCache("SELECT MAX(surname) FROM " + USER_TYPE + " GROUP BY name ORDER BY name ASC");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
      assertEquals("Doe", list.get(0)[0]);
      assertEquals("Woman", list.get(1)[0]);
   }

   public void testEmbeddedMax() {
      Query<Object[]> q = queryCache("SELECT u.surname, MAX(u.addresses.number) FROM " + USER_TYPE + " u GROUP BY u.surname ORDER BY u.surname ASC");
      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
      assertEquals(2, list.get(2).length);
      assertEquals(156, list.get(0)[1]);
      assertEquals(312, list.get(1)[1]);
      assertNull(list.get(2)[1]);
   }

   public void testEmbeddedMaxString() {
      Query<Object[]> q = queryCache("SELECT MAX(u.addresses.postCode) FROM " + USER_TYPE + " u GROUP BY u.name ORDER BY u.name ASC");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("X1234", list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertEquals("ZZ", list.get(1)[0]);
   }

   public void testGlobalMaxDouble() {
      Query<Object[]> q = queryCache("SELECT MAX(amount) FROM " + TRANSACTION_TYPE);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1800d, list.get(0)[0]);
   }

   public void testGlobalMaxString() {
      Query<Object[]> q = queryCache("SELECT MAX(name) FROM " + USER_TYPE);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("Spider", list.get(0)[0]);
   }

   public void testEmbeddedGlobalMax() {
      Query<Object[]> q = queryCache("SELECT MAX(u.addresses.number) FROM " + USER_TYPE + " u");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(312, list.get(0)[0]);
   }

   public void testOrderBySum() {
      Query<Object[]> q = queryCache("SELECT SUM(age) FROM " + USER_TYPE + " ORDER BY SUM(age) ASC");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(22L, list.get(0)[0]);
   }

   public void testGroupingWithFilter() {
      Query<Object[]> q = queryCache("SELECT name FROM " + USER_TYPE + " WHERE name = 'John' GROUP BY name HAVING name = 'John'");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("John", list.get(0)[0]);
   }

   public void testCountNull() {
      Query<Object[]> q = queryCache("SELECT COUNT(age) FROM " + USER_TYPE);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);  // only non-null "age"s were counted
   }

   public void testCountNull2() {
      Query<Object[]> q = queryCache("SELECT name, COUNT(age) FROM " + USER_TYPE + " GROUP BY name ORDER BY name ASC");
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
      Query<Object[]> q = queryCache("SELECT name, COUNT(salutation) FROM " + USER_TYPE + " GROUP BY name ORDER BY name ASC");
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
      Query<Object[]> q = queryCache("SELECT AVG(age) FROM " + USER_TYPE);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(22.0, list.get(0)[0]);  // only non-null "age"s were used in the average
   }

   public void testDateGrouping1() {
      Query<Object[]> q = queryCache("SELECT date FROM %s WHERE date BETWEEN '%s' AND '%s' GROUP BY date ",
            TRANSACTION_TYPE,
            queryDate(LocalDate.of(2013, 2, 15)),
            queryDate(LocalDate.of(2013, 3, 15))
      );
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(0, compareDate(list.get(0)[0], LocalDate.of(2013, 2, 27)));
   }

   public void testDateGrouping2() {
      Query<Object[]> q = queryCache("SELECT COUNT(date), MIN(date) FROM " + TRANSACTION_TYPE + " WHERE description = 'Hotel' GROUP BY id");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);
      assertEquals(0, compareDate(list.get(0)[1], LocalDate.of(2013, 2, 27)));
   }

   public void testDateGrouping3() {
      Query<Object[]> q = queryCache("SELECT MIN(date), COUNT(date) FROM " + TRANSACTION_TYPE + " WHERE description = 'Hotel' GROUP BY id");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(0, compareDate(list.get(0)[0], LocalDate.of(2013, 2, 27)));
      assertEquals(1L, list.get(0)[1]);
   }

   public void testParam() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE gender = :param2");
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
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE gender = :param1 AND name = :param2");

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

   public void testDateParam() {
      Query<Account> q = queryCache("FROM " + ACCOUNT_TYPE + " WHERE creationDate = :param1");
      q.setParameter("param1", queryDate(LocalDate.of(2013, 1, 3)));
      List<Account> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testParamWithGroupBy() {
      Query<Object[]> q = queryCache("SELECT accountId, date, SUM(amount) FROM " + TRANSACTION_TYPE + " GROUP BY accountId, date HAVING SUM(amount) > :param");
      q.setParameter("param", 1801);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).length);
      assertEquals(6225d, list.get(0)[2]);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028526:.*")
   public void testNullParamName() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name = :");
      q.setParameter(null, "John");
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "ISPN014825: Query parameter 'param2' was not set")
   public void testMissingParam() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name = :param1 AND gender = :param2");
      q.setParameter("param1", "John");
      q.list();
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "ISPN014825: Query parameter 'param2' was not set")
   public void testMissingParamWithParameterMap() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name = :param1 AND gender = :param2");
      Map<String, Object> parameterMap = new HashMap<>(1);
      parameterMap.put("param1", "John");
      q.setParameters(parameterMap);
      q.list();
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN014812: paramValues cannot be null")
   public void testQueryWithNoParamsWithNullParameterMap() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE name = 'John'");
      q.setParameters(null);
   }

   @Test
   public void testComplexQuery() throws Exception {
      Query<Object[]> q = queryCache("SELECT AVG(amount), SUM(amount), COUNT(date), MIN(date), MAX(accountId) FROM " + TRANSACTION_TYPE + " WHERE isDebit = :param ORDER BY AVG(amount) ASC, COUNT(date) DESC, MAX(amount) ASC");
      q.setParameter("param", true);
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(5, list.get(0).length);
      assertEquals(143.50909d, (Double) list.get(0)[0], 0.0001d);
      assertEquals(7893d, (Double) list.get(0)[1], 0.0001d);
      assertEquals(55L, list.get(0)[2]);
      assertEquals(0, compareDate(list.get(0)[3], LocalDate.of(2013, 1, 1)));
      assertEquals(2, list.get(0)[4]);
   }

   public void testDateFilteringWithGroupBy() {
      Query<Object[]> q = queryCache("SELECT date FROM %s WHERE date BETWEEN '%s' AND '%s' GROUP BY date",
            TRANSACTION_TYPE,
            queryDate(LocalDate.of(2013, 2, 15)),
            queryDate(LocalDate.of(2013, 3, 15))
      );
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(0, compareDate(list.get(0)[0], LocalDate.of(2013, 2, 27)));
   }

   public void testAggregateDate() {
      Query<Object[]> q = queryCache("SELECT COUNT(date), MIN(date) FROM " + TRANSACTION_TYPE + " WHERE description = 'Hotel' GROUP BY id");
      List<Object[]> list = q.list();

      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);
      assertEquals(0, compareDate(list.get(0)[1], LocalDate.of(2013, 2, 27)));
   }

   public void testNotIndexedProjection() {
      Query<Object[]> q = queryCache("SELECT id, isValid FROM " + TRANSACTION_TYPE + " WHERE id >= 98 ORDER BY id ASC");
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
      Query<Object[]> q = queryCache("SELECT id, description FROM " + TRANSACTION_TYPE + " WHERE id >= 98 ORDER BY id ASC");
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
      Query<Object[]> q = queryCache("SELECT id, isValid FROM " + TRANSACTION_TYPE + " WHERE id >= 98 ORDER BY isValid ASC, id ASC");
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
      Query<Object[]> q = queryCache("SELECT id, description FROM " + TRANSACTION_TYPE + " WHERE id >= 98 ORDER BY description ASC, id ASC");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(98, list.get(0)[0]);
      assertEquals("Expensive shoes 48", list.get(0)[1]);
      assertEquals(2, list.get(1).length);
      assertEquals(99, list.get(1)[0]);
      assertEquals("Expensive shoes 49", list.get(1)[1]);
   }

   public void testDuplicateDateProjection() {
      Query<Object[]> q = queryCache("SELECT id, date, date FROM " + TRANSACTION_TYPE + " WHERE description = 'Hotel'");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(3, list.get(0).length);
      assertEquals(3, list.get(0)[0]);
      assertEquals(0, compareDate(list.get(0)[1], LocalDate.of(2013, 2, 27)));
      assertEquals(0, compareDate(list.get(0)[2], LocalDate.of(2013, 2, 27)));
   }

   public void testDuplicateBooleanProjection() {
      Query<Object[]> q = queryCache("SELECT id, isDebit, isDebit FROM " + TRANSACTION_TYPE + " WHERE description = 'Hotel'");
      List<Object[]> list = q.list();

      assertEquals(1, list.size());
      assertEquals(3, list.get(0).length);
      assertEquals(3, list.get(0)[0]);
      assertEquals(true, list.get(0)[1]);
      assertEquals(true, list.get(0)[2]);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014023: Using the multi-valued property path 'addresses.street' in the GROUP BY clause is not currently supported")
   public void testGroupByMustNotAcceptRepeatedProperty() {
      Query<Object[]> q = queryCache("SELECT MIN(u.name) FROM " + USER_TYPE + " u GROUP BY u.addresses.street");
      q.list();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014024: The property path 'addresses.street' cannot be used in the ORDER BY clause because it is multi-valued")
   public void testOrderByMustNotAcceptRepeatedProperty() {
      Query<Object[]> q = queryCache("SELECT u.name FROM " + USER_TYPE + " u ORDER BY u.addresses.street ASC");
      q.list();
   }

   public void testOrderByInAggregationQueryMustAcceptRepeatedProperty() {
      Query<Object[]> q = queryCache("SELECT AVG(u.age), u.name FROM " + USER_TYPE + " u WHERE u.name > 'A' GROUP BY u.name HAVING MAX(u.addresses.street) > 'A' ORDER BY MIN(u.addresses.street) ASC");
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
      Query<User> q = queryCache("FROM " + USER_TYPE + " u WHERE u.name = MIN(u.addresses.street)");
      q.list();
   }

   public void testAggregateRepeatedField() {
      Query<Object[]> q = queryCache("SELECT MIN(u.addresses.street) FROM " + USER_TYPE + " u WHERE u.name = 'Spider' ");
      List<Object[]> list = q.list();
      assertEquals("Bond Street", list.get(0)[0]);
   }

   public void testGroupingAndAggregationOnSameField() {
      Query<Object[]> q = queryCache("SELECT COUNT(surname) FROM " + USER_TYPE + " GROUP BY surname");
      List<Object[]> list = q.list();
      assertEquals(3, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);
      assertEquals(1L, list.get(1)[0]);
      assertEquals(1L, list.get(2)[0]);
   }

   public void testTwoPhaseGroupingAndAggregationOnSameField() {
      Query<Object[]> q = queryCache("SELECT COUNT(u.surname), SUM(u.addresses.number) FROM " + USER_TYPE + " u GROUP BY u.surname ORDER BY u.surname ASC");
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
   public void testLuceneWildcardsAreEscaped() {      // use a true wildcard
      Query<User> q1 = queryCache("FROM " + USER_TYPE + " WHERE name LIKE 'J%n'");
      assertEquals(1, q1.list().size());

      // use an improper wildcard
      Query<User> q2 = queryCache("FROM " + USER_TYPE + " WHERE name LIKE 'J*n'");
      assertEquals(0, q2.list().size());

      // use a true wildcard
      Query<User> q3 = queryCache("FROM " + USER_TYPE + " WHERE name LIKE 'Jo_n'");
      assertEquals(1, q3.list().size());

      // use an improper wildcard
      Query<User> q4 = queryCache("FROM " + USER_TYPE + " WHERE name LIKE 'Jo?n'");
      assertEquals(0, q4.list().size());
   }

   public void testCompareLongWithInt() {
      Query<Object[]> q = queryCache("SELECT SUM(age) FROM " + USER_TYPE + " GROUP BY name HAVING SUM(age) > 50000");
      List<Object[]> list = q.list();
      assertEquals(0, list.size());
   }

   public void testCompareDoubleWithInt() {
      Query<Object[]> q = queryCache("SELECT SUM(amount) FROM " + TRANSACTION_TYPE + " GROUP BY accountId HAVING SUM(amount) > 50000");
      List<Object[]> list = q.list();
      assertEquals(0, list.size());
   }

   public void testFullTextTerm() {
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE longDescription:'rent'");
      List<Transaction> list = q.list();
      assertEquals(1, list.size());
   }

   public void testFullTextPhrase() {
      Query<Transaction> q = queryCache("FROM " + TRANSACTION_TYPE + " WHERE longDescription:'expensive shoes'");
      List<Transaction> list = q.list();
      assertEquals(50, list.size());
   }

   public void testInstant1() {
      Query<User> q = queryCache("FROM %s WHERE creationDate = %s", USER_TYPE, instant(Instant.parse("2011-12-03T10:15:30Z")));
      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testInstant2() {
      Query<User> q = queryCache("FROM %s WHERE passwordExpirationDate = %s", USER_TYPE, instant(Instant.parse("2011-12-03T10:15:30Z")));
      List<User> list = q.list();
      assertEquals(3, list.size());
   }


   public void testSingleIN() {
      Query<User> q = queryCache("FROM " + USER_TYPE + " WHERE surname IN ('Man') AND gender = 'MALE'");
      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getId());
      assertEquals("Man", list.get(0).getSurname());
      assertEquals(User.Gender.MALE, list.get(0).getGender());
   }
}

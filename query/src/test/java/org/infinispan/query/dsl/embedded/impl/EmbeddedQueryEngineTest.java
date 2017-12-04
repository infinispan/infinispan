package org.infinispan.query.dsl.embedded.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.TimeZone;

import org.hibernate.search.exception.SearchException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParser;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.embedded.impl.model.TheEntity;
import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.Author;
import org.infinispan.query.dsl.embedded.testdomain.Book;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AddressHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.impl.EmbeddedQueryEngineTest")
@CleanupAfterTest
public class EmbeddedQueryEngineTest extends MultipleCacheManagersTest {

   private final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

   private QueryEngine<Class<?>> qe;

   public EmbeddedQueryEngineTest() {
      DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
   }

   private Date makeDate(String dateStr) throws ParseException {
      return DATE_FORMAT.parse(dateStr);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().index(Index.ALL)
            .addIndexedEntity(UserHS.class)
            .addIndexedEntity(AccountHS.class)
            .addIndexedEntity(TransactionHS.class)
            .addIndexedEntity(TheEntity.class)
            .addIndexedEntity(Book.class)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(1, cfg);
   }

   @BeforeClass(alwaysRun = true)
   protected void init() throws Exception {
      qe = new EmbeddedQueryEngine(cache(0).getAdvancedCache(), true);

      // create the test objects
      User user1 = new UserHS();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("Doe");
      user1.setGender(User.Gender.MALE);
      user1.setAge(22);
      user1.setAccountIds(new HashSet<>(Arrays.asList(1, 2)));
      user1.setNotes("Lorem ipsum dolor sit amet");

      Address address1 = new AddressHS();
      address1.setStreet("Main Street");
      address1.setPostCode("X1234");
      user1.setAddresses(Collections.singletonList(address1));

      User user2 = new UserHS();
      user2.setId(2);
      user2.setName("Spider");
      user2.setSurname("Man");
      user2.setGender(User.Gender.MALE);
      user2.setAge(44);
      user2.setAccountIds(Collections.singleton(3));

      Address address2 = new AddressHS();
      address2.setStreet("Old Street");
      address2.setPostCode("Y12");
      Address address3 = new AddressHS();
      address3.setStreet("Bond Street");
      address3.setPostCode("ZZ");
      user2.setAddresses(Arrays.asList(address2, address3));

      User user3 = new UserHS();
      user3.setId(3);
      user3.setName("Spider");
      user3.setSurname("Woman");
      user3.setGender(User.Gender.FEMALE);
      user3.setAccountIds(Collections.emptySet());

      Account account1 = new AccountHS();
      account1.setId(1);
      account1.setDescription("John Doe's first bank account");
      account1.setCreationDate(makeDate("2013-01-03"));

      Account account2 = new AccountHS();
      account2.setId(2);
      account2.setDescription("John Doe's second bank account");
      account2.setCreationDate(makeDate("2013-01-04"));

      Account account3 = new AccountHS();
      account3.setId(3);
      account3.setCreationDate(makeDate("2013-01-20"));

      Transaction transaction0 = new TransactionHS();
      transaction0.setId(0);
      transaction0.setDescription("Birthday present");
      transaction0.setAccountId(1);
      transaction0.setAmount(1800);
      transaction0.setDate(makeDate("2012-09-07"));
      transaction0.setDebit(false);
      transaction0.setValid(true);

      Transaction transaction1 = new TransactionHS();
      transaction1.setId(1);
      transaction1.setDescription("Feb. rent payment");
      transaction1.setAccountId(1);
      transaction1.setAmount(1500);
      transaction1.setDate(makeDate("2013-01-05"));
      transaction1.setDebit(true);
      transaction1.setValid(true);

      Transaction transaction2 = new TransactionHS();
      transaction2.setId(2);
      transaction2.setDescription("Starbucks");
      transaction2.setAccountId(1);
      transaction2.setAmount(23);
      transaction2.setDate(makeDate("2013-01-09"));
      transaction2.setDebit(true);
      transaction2.setValid(true);

      Transaction transaction3 = new TransactionHS();
      transaction3.setId(3);
      transaction3.setDescription("Hotel");
      transaction3.setAccountId(2);
      transaction3.setAmount(45);
      transaction3.setDate(makeDate("2013-02-27"));
      transaction3.setDebit(true);
      transaction3.setValid(true);

      Transaction transaction4 = new TransactionHS();
      transaction4.setId(4);
      transaction4.setDescription("Last january");
      transaction4.setAccountId(2);
      transaction4.setAmount(95);
      transaction4.setDate(makeDate("2013-01-31"));
      transaction4.setDebit(true);
      transaction4.setValid(true);

      Transaction transaction5 = new TransactionHS();
      transaction5.setId(5);
      transaction5.setDescription("Popcorn");
      transaction5.setAccountId(2);
      transaction5.setAmount(5);
      transaction5.setDate(makeDate("2013-01-01"));
      transaction5.setDebit(true);
      transaction5.setValid(true);

      // persist and index the test objects
      // we put all of them in the same cache for the sake of simplicity
      cache(0).put("user_" + user1.getId(), user1);
      cache(0).put("user_" + user2.getId(), user2);
      cache(0).put("user_" + user3.getId(), user3);
      cache(0).put("account_" + account1.getId(), account1);
      cache(0).put("account_" + account2.getId(), account2);
      cache(0).put("account_" + account3.getId(), account3);
      cache(0).put("transaction_" + transaction0.getId(), transaction0);
      cache(0).put("transaction_" + transaction1.getId(), transaction1);
      cache(0).put("transaction_" + transaction2.getId(), transaction2);
      cache(0).put("transaction_" + transaction3.getId(), transaction3);
      cache(0).put("transaction_" + transaction4.getId(), transaction4);
      cache(0).put("transaction_" + transaction5.getId(), transaction5);

      for (int i = 0; i < 50; i++) {
         Transaction transaction = new TransactionHS();
         transaction.setId(50 + i);
         transaction.setDescription("Expensive shoes " + i);
         transaction.setAccountId(2);
         transaction.setAmount(100 + i);
         transaction.setDate(makeDate("2013-08-20"));
         transaction.setDebit(true);
         transaction.setValid(true);
         cache(0).put("transaction_" + transaction.getId(), transaction);
      }

      // this value should be ignored gracefully
      cache(0).put("dummy", "a primitive value cannot be queried");

      cache(0).put("notIndexed1", new NotIndexed("testing 123"));
      cache(0).put("notIndexed2", new NotIndexed("xyz"));

      cache(0).put("entity1", new TheEntity("test value 1", new TheEntity.TheEmbeddedEntity("test embedded value 1")));
      cache(0).put("entity2", new TheEntity("test value 2", new TheEntity.TheEmbeddedEntity("test embedded value 2")));

      cache(0).put("book1", new Book("Java Performance: The Definitive Guide", "O'Reilly Media", new Author("Scott", "Oaks"),
            "Still, it turns out that every day, I think about GC performance, or the\n" +
                  "performance of the JVM compiler, or how to get the best performance from Java Enterprise Edition APIs."));

      cache(0).put("book2", new Book("Functional Programming for Java Developers", "O'Reilly Media", new Author("Dean", "Wampler"),
            "Why should a Java developer learn about functional programming (FP)? After all, hasn’t\n" +
                  "functional programming been safely hidden in academia for decades? Isn’t object-\n" +
                  "oriented programming (OOP) all we really need?"));

      cache(0).put("book3", new Book("The Java ® Virtual Machine Specification Java SE 8 Edition", "Oracle", new Author("Tim", "Lindholm"),
            "The Java SE 8 Edition of The Java Virtual Machine Specification incorporates all the changes that have " +
                  "been made to the Java Virtual Machine since the Java SE 7 Edition in 2011. In addition, numerous " +
                  "corrections and clarifications have been made to align with popular implementations of the Java Virtual Machine."));
   }

   @Override
   protected void clearContent() {
      // Don't clear, this is destroying the index
   }

   private Query buildQuery(String queryString) {
      return qe.buildQuery(null, qe.parse(queryString), null, -1, -1);
   }

   public void testSimpleProjection1() {
      IckleParsingResult<Class<?>> parsingResult = IckleParser.parse("select b.author.name from org.infinispan.query.dsl.embedded.testdomain.Book b", qe.propertyHelper);
      CacheQuery q = qe.buildLuceneQuery(parsingResult, null, -1, -1);

      List<?> list = q.list();
      assertEquals(3, list.size());
   }

   @Test(enabled = false, description = "Disabled due to https://issues.jboss.org/browse/ISPN-8564")
   public void testSimpleProjection2() {
      IckleParsingResult<Class<?>> parsingResult = IckleParser.parse("select author.name from org.infinispan.query.dsl.embedded.testdomain.Book", qe.propertyHelper);
      CacheQuery q = qe.buildLuceneQuery(parsingResult, null, -1, -1);

      List<?> list = q.list();
      assertEquals(3, list.size());
   }

   public void testGrouping() {
      Query q = buildQuery("select name from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS " +
            "where surname is not null group by name having name >= 'A'");
      List<User> list = q.list();
      assertEquals(2, list.size());
   }

   public void testNoGroupingOrAggregation() {
      Query q = buildQuery("from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS");
      assertTrue(q instanceof EmbeddedLuceneQuery);
      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028516: Cannot have aggregate functions in the GROUP BY clause : SUM.")
   public void testDisallowAggregationInGroupBy() {
      Query q = buildQuery("select sum(age) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS group by sum(age) ");
      q.list();
   }

   public void testDuplicatesAcceptedInGroupBy() {
      Query q = buildQuery("select name from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS group by name, name");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1, list.get(1).length);
   }

   public void testDuplicatesAcceptedInSelect1() {
      Query q = buildQuery("select name, name from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS group by name");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(2, list.get(1).length);
   }

   public void testDuplicatesAcceptedInSelect2() {
      Query q = buildQuery("select max(name), max(name) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
   }

   public void testDuplicatesAcceptedInSelect3() {
      Query q = buildQuery("select min(name), max(name) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
   }

   public void testDuplicatesAcceptedInOrderBy1() {
      Query q = buildQuery("from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS order by age, age");
      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testDuplicatesAcceptedInOrderBy2() {
      Query q = buildQuery("from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS order by age, name, age");
      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014024: The property path 'addresses.postCode' cannot be used in the ORDER BY clause because it is multi-valued")
   public void testRejectMultivaluedOrderBy() {
      Query q = buildQuery("from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS u order by u.addresses.postCode");
      q.list();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014023: Using the multi-valued property path 'addresses.postCode' in the GROUP BY clause is not currently supported")
   public void testRejectMultivaluedGroupBy() {
      Query q = buildQuery("select u.addresses.postCode from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS u group by u.addresses.postCode");
      q.list();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014026: The expression 'age' must be part of an aggregate function or it should be included in the GROUP BY clause")
   public void testMissingAggregateInSelect() {
      Query q = buildQuery("select age from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS group by name");
      q.list();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014026: The expression 'age' must be part of an aggregate function or it should be included in the GROUP BY clause")
   public void testMissingAggregateInOrderBy() {
      Query q = buildQuery("select name, sum(age) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS group by name order by age");
      q.list();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028515: Cannot have aggregate functions in the WHERE clause : SUM.")
   public void testDisallowAggregatesInWhereClause() {
      Query q = buildQuery("select name from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS where sum(age) > 33 group by name");
      q.list();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014026: The expression 'age' must be part of an aggregate function or it should be included in the GROUP BY clause")
   public void testHavingClauseAllowsAggregationsAndGroupByColumnsOnly() {
      Query q = buildQuery("select name from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS group by name having age >= 18");
      q.list();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014026: The expression 'name' must be part of an aggregate function or it should be included in the GROUP BY clause")
   public void testDisallowNonAggregatedProjectionWithGlobalAggregation() {
      Query q = buildQuery("select name, count(name) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS");
      q.list();
   }

   public void testBuildLuceneQuery() {
      IckleParsingResult<Class<?>> parsingResult = IckleParser.parse("select name from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS", qe.propertyHelper);
      CacheQuery<UserHS> q = qe.buildLuceneQuery(parsingResult, null, -1, -1);
      List<?> list = q.list();
      assertEquals(3, list.size());
   }

   @Test(expectedExceptions = SearchException.class, expectedExceptionsMessageRegExp = "Unable to find field notes in org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS")
   public void testBuildLuceneQueryOnNonIndexedField() {
      IckleParsingResult<Class<?>> parsingResult = IckleParser.parse("select notes from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS where notes like 'TBD%'", qe.propertyHelper);
      CacheQuery<?> q = qe.buildLuceneQuery(parsingResult, null, -1, -1);
   }

   public void testGlobalCount() {
      Query q = buildQuery("select count(name), count(age) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0).length);
      assertEquals(3L, list.get(0)[0]);
      assertEquals(2L, list.get(0)[1]);
   }

   public void testGlobalAvg() {
      Query q = buildQuery("select avg(age) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(33.0d, list.get(0)[0]);
   }

   public void testGlobalSum() {
      Query q = buildQuery("select sum(age) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(66L, list.get(0)[0]);
   }

   public void testGlobalMin() {
      Query q = buildQuery("select min(age) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(22, list.get(0)[0]);
   }

   public void testGlobalMax() {
      Query q = buildQuery("select max(age) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS");
      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(44, list.get(0)[0]);
   }

   public void testAggregateGroupingField() {
      Query q = buildQuery("select count(name) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS group by name order by count(name)");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(1L, list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertEquals(2L, list.get(1)[0]);
   }

   public void testAggregateEmbedded1() {
      Query q = buildQuery("select max(accountIds) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS group by name order by name");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals(2, list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertEquals(3, list.get(1)[0]);
   }

   public void testAggregateEmbedded2() {
      Query q = buildQuery("select max(u.addresses.postCode) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS u group by u.name order by u.name");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("X1234", list.get(0)[0]);
      assertEquals(1, list.get(1).length);
      assertEquals("ZZ", list.get(1)[0]);
   }

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Aggregation SUM cannot be applied to property of type java.lang.String")
   public void testIncompatibleAggregator() {
      Query q = buildQuery("select sum(name) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS");
      q.list();
   }

   public void testAggregateNulls() {
      Query q = buildQuery("select name, sum(age), avg(age) from org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS " +
            "where surname is not null " +
            "group by name " +
            "having name >= 'A' and count(age) >= 1");
      List<User> list = q.list();
      assertEquals(2, list.size());
   }

   public void testRenamedFields1() {
      Query q = buildQuery("select theField from org.infinispan.query.dsl.embedded.impl.model.TheEntity where theField >= 'a' order by theField");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("test value 1", list.get(0)[0]);
      assertEquals("test value 2", list.get(1)[0]);
   }

   public void testRenamedFields2() {
      Query q = buildQuery("select theField from org.infinispan.query.dsl.embedded.impl.model.TheEntity order by theField");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("test value 1", list.get(0)[0]);
      assertEquals("test value 2", list.get(1)[0]);
   }

   public void testRenamedFields3() {
      Query q = buildQuery("select e.embeddedEntity.anotherField from org.infinispan.query.dsl.embedded.impl.model.TheEntity e where e.embeddedEntity.anotherField >= 'a' order by e.theField");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("test embedded value 1", list.get(0)[0]);
      assertEquals("test embedded value 2", list.get(1)[0]);
   }

   public void testRenamedFields4() {
      Query q = buildQuery("select e.embeddedEntity.anotherField from org.infinispan.query.dsl.embedded.impl.model.TheEntity e order by e.theField");
      List<Object[]> list = q.list();
      assertEquals(2, list.size());
      assertEquals(1, list.get(0).length);
      assertEquals("test embedded value 1", list.get(0)[0]);
      assertEquals("test embedded value 2", list.get(1)[0]);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028507: Invalid boolean literal '90'")
   public void testInvalidNotIndexedBooleanComparison() {
      buildQuery("from org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS where isValid = 90");
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014037: Invalid boolean literal '90'")
   public void testInvalidIndexedBooleanComparison() {
      buildQuery("from org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS where isDebit = 90");
   }

   public void testBooleanComparison() {
      IckleParsingResult<Class<?>> parsingResult = IckleParser.parse("from org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS " +
            "WHERE isDebit = false", qe.propertyHelper);
      CacheQuery q = qe.buildLuceneQuery(parsingResult, null, -1, -1);

      List<?> list = q.list();
      assertEquals(1, list.size());
   }

   public void testConstantBooleanExpression() {
      IckleParsingResult<Class<?>> parsingResult = IckleParser.parse("from org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS " +
            "WHERE true", qe.propertyHelper);
      CacheQuery q = qe.buildLuceneQuery(parsingResult, null, -1, -1);

      List<?> list = q.list();
      assertEquals(56, list.size());

      parsingResult = IckleParser.parse("from org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS " +
            "WHERE false", qe.propertyHelper);
      q = qe.buildLuceneQuery(parsingResult, null, -1, -1);

      list = q.list();
      assertEquals(0, list.size());
   }

   public void testFullTextKeyword() {
      IckleParsingResult<Class<?>> parsingResult = IckleParser.parse("from org.infinispan.query.dsl.embedded.testdomain.Book b " +
            "where b.preface:('java se'^7 -('bicycle' 'ski')) and b.publisher:'Oracel'~2", qe.propertyHelper);
      CacheQuery q = qe.buildLuceneQuery(parsingResult, null, -1, -1);

      List<?> list = q.list();
      assertEquals(1, list.size());
   }
}

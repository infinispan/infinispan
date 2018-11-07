package org.infinispan.query.dsl.embedded;

import static org.testng.AssertJUnit.assertEquals;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test string-based queries.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
@Test(groups = {"functional", "smoke"}, testName = "query.dsl.embedded.QueryStringTest")
public class QueryStringTest extends AbstractQueryDslTest {

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

   @BeforeClass(alwaysRun = true)
   protected void populateCache() throws Exception {
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
      user3.setGender(User.Gender.FEMALE);
      user3.setAccountIds(Collections.emptySet());
      user3.setCreationDate(Instant.parse("2011-12-03T10:15:30Z"));
      user3.setPasswordExpirationDate(Instant.parse("2011-12-03T10:15:30Z"));
      user3.setAddresses(new ArrayList<>());

      Transaction transaction0 = getModelFactory().makeTransaction();
      transaction0.setId(0);
      transaction0.setDescription("Birthday present");
      transaction0.setAccountId(1);
      transaction0.setAmount(1800);
      transaction0.setDate(makeDate("2012-09-07"));
      transaction0.setDebit(false);
      transaction0.setNotes("card was not present");
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
         transaction.setLongDescription("Expensive shoes. Just beer, really " + i);
         transaction.setAccountId(2);
         transaction.setAmount(100 + i);
         transaction.setDate(makeDate("2013-08-20"));
         transaction.setDebit(true);
         transaction.setValid(true);
         getCacheForWrite().put("transaction_" + transaction.getId(), transaction);
      }

      getCacheForWrite().put("notIndexed1", new NotIndexed("testing 123"));
      getCacheForWrite().put("notIndexed2", new NotIndexed("xyz"));
   }

   public void testParam() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " where id = :idParam");

      q.setParameter("idParam", 1);

      List<Transaction> list = q.list();

      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());

      q.setParameter("idParam", 2);

      list = q.list();

      assertEquals(1, list.size());
      assertEquals(2, list.get(0).getId());
   }

   @Test(enabled = false)
   public void testParamWithSpacePadding() {
      //todo [anistor] need special tree nodes for all literal types (and for params) to be able to distinguish them better; QueryRendererDelegate.predicateXXX should receive such a tree node instead of a string
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " where id = :  idParam");
      List<Transaction> list = q.list();
      assertEquals(1, list.size());
   }

   public void testExactMatch() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " where description = 'Birthday present'");

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
   }

   public void testFullTextTerm() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " where longDescription:'rent'");

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
   }

   public void testFullTextTermRightOperandAnalyzed() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " where longDescription:'RENT'");

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
   }

   public void testFullTextTermBoost() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " where longDescription:('rent'^8 'shoes')");

      List<Transaction> list = q.list();
      assertEquals(51, list.size());
   }

   public void testFullTextPhrase() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " where longDescription:'expensive shoes'");

      List<Transaction> list = q.list();
      assertEquals(50, list.size());
   }

   public void testFullTextWithAggregation() {
      Query q = createQueryFromString("select t.accountId, max(t.amount), max(t.description) from " + getModelFactory().getTransactionTypeName()
            + " t where t.longDescription : (+'beer' && -'food') group by t.accountId");

      List<Object[]> list = q.list();
      assertEquals(1, list.size());
      assertEquals(2, list.get(0)[0]);
      assertEquals(149.0, list.get(0)[1]);
      assertEquals("Expensive shoes 9", list.get(0)[2]);
   }

   public void testFullTextTermBoostAndSorting() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " where longDescription:('rent'^8 'shoes') order by amount");

      List<Transaction> list = q.list();
      assertEquals(51, list.size());
   }

   public void testFullTextTermOccur() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where not (t.longDescription : (+'failed') or t.longDescription : 'blocked')");

      List<Transaction> list = q.list();
      assertEquals(56, list.size());
   }

   public void testFullTextTermDoesntOccur() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : (-'really')");

      List<Transaction> list = q.list();
      assertEquals(6, list.size());
   }

   public void testFullTextRangeWildcard() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : [* to *]");

      List<Transaction> list = q.list();
      assertEquals(54, list.size());
   }

   public void testFullTextRange() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.amount : [23 to 45]");

      List<Transaction> list = q.list();
      assertEquals(2, list.size());
   }

   public void testFullTextPrefix() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : 'ren*'");

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
   }

   public void testFullTextWildcard() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : 're?t'");

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN014036: Prefix, wildcard or regexp queries cannot be fuzzy.*")
   public void testFullTextWildcardFuzzyNotAllowed() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : 're?t'~2");

      q.list();
   }

   public void testFullTextFuzzy() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : 'retn'~");

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
   }

   public void testFullTextFuzzyDefaultEdits() {
      // default number of edits should be 2
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : 'ertn'~");

      List<Transaction> list = q.list();
      assertEquals(1, list.size());

      q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : 'ajunayr'~");

      list = q.list();
      assertEquals(0, list.size());
   }

   public void testFullTextFuzzySpecifiedEdits() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : 'ajnuary'~1");

      List<Transaction> list = q.list();
      assertEquals(1, list.size());

      q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : 'ajunary'~1");

      list = q.list();
      assertEquals(0, list.size());
   }

   public void testFullTextRegexp() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : /[R|r]ent/");

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028526: Invalid query.*")
   public void testFullTextRegexpFuzzyNotAllowed() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : /[R|r]ent/~2");

      q.list();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028522: .*property is analyzed.*")
   public void testExactMatchOnAnalyzedFieldNotAllowed() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " where longDescription = 'Birthday present'");

      q.list();
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: .*unless the property is indexed and analyzed.*")
   public void testFullTextTermOnNonAnalyzedFieldNotAllowed() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " where description:'rent'");

      q.list();
   }

   @Test(enabled = false) //TODO [anistor] fix!
   public void testFullTextRegexp2() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " t where t.longDescription : ( -'beer' and '*')");

      List<Transaction> list = q.list();
      assertEquals(1, list.size());
   }

   public void testInstant1() {
      Query q = createQueryFromString("from " + getModelFactory().getUserTypeName() + " u where u.creationDate = '2011-12-03T10:15:30Z'");

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testInstant2() {
      Query q = createQueryFromString("from " + getModelFactory().getUserTypeName() + " u where u.passwordExpirationDate = '2011-12-03T10:15:30Z'");

      List<User> list = q.list();
      assertEquals(3, list.size());
   }

   public void testEqNonIndexedType() {
      Query q = createQueryFromString("from " + NotIndexed.class.getName() + " where notIndexedField = 'testing 123'");

      List<NotIndexed> list = q.list();
      assertEquals(1, list.size());
      assertEquals("testing 123", list.get(0).notIndexedField);
   }

   /**
    * See <a href="https://issues.jboss.org/browse/ISPN-7863">ISPN-7863</a>
    */
   public void testAliasContainingLetterV() {
      Query q = createQueryFromString("from " + getModelFactory().getTransactionTypeName() + " vvv where vvv.description = 'Birthday present'");
      assertEquals(1, q.list().size());
   }

   protected Query createQueryFromString(String q) {
      return getQueryFactory().create(q);
   }
}

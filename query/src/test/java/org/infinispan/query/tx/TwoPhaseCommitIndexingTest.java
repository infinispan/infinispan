package org.infinispan.query.tx;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.hibernate.search.exception.SearchException;
import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.tx.TwoPhaseCommitIndexingTest")
public class TwoPhaseCommitIndexingTest extends SingleCacheManagerTest {

   private final AtomicBoolean injectFailures = new AtomicBoolean();
   private final BlowUpInterceptor nastyInterceptor = new BlowUpInterceptor(injectFailures);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .customInterceptors()
            .addInterceptor()
               .after(EntryWrappingInterceptor.class)
               .interceptor(nastyInterceptor)
         .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .use1PcForAutoCommitTransactions(false)
         .indexing()
            .index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testQueryAfterAddingNewNode() throws Exception {
      //We'll fail the first operation by having an exception thrown after prepare
      //but before the commit:
      store("Astronaut", new Person("Astronaut","is asking his timezone", 32), true);
      //Nothing should be applied to the indexes:
      assertFind("timezone", 0);
      assertFind("asking", 0);
      assertFind("cat", 0);

      store("Astronaut", new Person("Astronaut","is asking his timezone", 32), false);
      assertFind("timezone", 1);
      assertFind("asking", 1);
      assertFind("cat", 0);
   }

   private void assertFind(String keyword, int expectedCount) {
      assertFind(cache, keyword, expectedCount);
   }

   private static void assertFind(Cache cache, String keyword, int expectedCount) {
      SearchManager queryFactory = Search.getSearchManager(cache);
      Query luceneQuery = new TermQuery(new Term("blurb", keyword));
      CacheQuery cacheQuery = queryFactory.getQuery(luceneQuery, Person.class);
      int resultSize = cacheQuery.getResultSize();
      Assert.assertEquals(resultSize, expectedCount);
   }

   private void store(final String key, final Object value, boolean failTheOperation) {
      if (failTheOperation) {
         injectFailures.set(true);
         try {
            cache.put(key, value);
            Assert.fail("Should have failed the implicit transaction");
         }
         catch (Exception e) {
            //Expected
         }
         finally {
            injectFailures.set(false);
         }
      }
      else {
         cache.put(key, value);
      }
   }

   private static class BlowUpInterceptor extends CommandInterceptor {

      private final AtomicBoolean injectFailures;

      public BlowUpInterceptor(AtomicBoolean injectFailures) {
         this.injectFailures = injectFailures;
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         if (injectFailures.get()) {
            throw new SearchException("Test");
         }
         else {
            return super.visitPrepareCommand(ctx, command);
         }
      }
   }

}

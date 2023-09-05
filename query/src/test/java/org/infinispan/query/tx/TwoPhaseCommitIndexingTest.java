package org.infinispan.query.tx;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hibernate.search.util.common.SearchException;
import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
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
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class)
         .locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   public void testQueryAfterAddingNewNode() throws Exception {
      //We'll fail the first operation by having an exception thrown after prepare
      //but before the commit:
      store("Astronaut", new Person("Astronaut", "is asking his timezone", 32), true);
      //Nothing should be applied to the indexes:
      assertFind("timezone", 0);
      assertFind("asking", 0);
      assertFind("cat", 0);

      store("Astronaut", new Person("Astronaut", "is asking his timezone", 32), false);
      assertFind("timezone", 1);
      assertFind("asking", 1);
      assertFind("cat", 0);
   }

   private void assertFind(String keyword, int expectedCount) {
      assertFind(cache, keyword, expectedCount);
   }

   private static void assertFind(Cache cache, String keyword, int expectedCount) {
      String q = String.format("FROM %s WHERE blurb:'%s'", Person.class.getName(), keyword);
      Query<?> cacheQuery = cache.query(q);
      int resultSize = cacheQuery.execute().count().value();
      Assert.assertEquals(resultSize, expectedCount);
   }

   private void store(final String key, final Object value, boolean failTheOperation) {
      if (failTheOperation) {
         injectFailures.set(true);
         try {
            cache.put(key, value);
            Assert.fail("Should have failed the implicit transaction");
         } catch (Exception e) {
            //Expected
         } finally {
            injectFailures.set(false);
         }
      } else {
         cache.put(key, value);
      }
   }

   static class BlowUpInterceptor extends DDAsyncInterceptor {

      private final AtomicBoolean injectFailures;

      public BlowUpInterceptor(AtomicBoolean injectFailures) {
         this.injectFailures = injectFailures;
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         if (injectFailures.get()) {
            throw new SearchException("Test");
         } else {
            return super.visitPrepareCommand(ctx, command);
         }
      }
   }

}

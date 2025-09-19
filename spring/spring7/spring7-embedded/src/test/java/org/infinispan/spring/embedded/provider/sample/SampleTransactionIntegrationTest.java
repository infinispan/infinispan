package org.infinispan.spring.embedded.provider.sample;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.embedded.provider.sample.entity.Book;
import org.infinispan.spring.embedded.provider.sample.service.CachedTransactionBookService;
import org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.testng.AbstractTransactionalTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Transaction integration tests. Verifies transaction manager functioning across DB/service/cache level.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
@Test(testName = "spring.embedded.provider.SampleTransactionIntegrationTest", groups = "functional")
@ContextConfiguration(locations = "classpath:/org/infinispan/spring/embedded/provider/sample/SampleTransactionIntegrationTestConfig.xml")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestExecutionListeners(value = InfinispanTestExecutionListener.class,  mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class SampleTransactionIntegrationTest extends AbstractTransactionalTestNGSpringContextTests {

   @Qualifier(value = "cachedTransactionBookServiceImpl")
   @Autowired
   private CachedTransactionBookService bookService;

   @Autowired
   private SpringEmbeddedCacheManager cacheManager;

   @AfterMethod
   public void clean() {
      transactionalCache().clear();
      nonTransactionalCache().clear();
   }

   @Test
   public void testUsingTemplate() throws SystemException {
      final Book[] created = new Book[2];
      final Book[] found = new Book[2];

      TransactionManager tm = getTransactionManager("booksTransactional");
      assert tm.getStatus() == 0 : "Transaction should be in state 'RUNNING'";

      Book nonTxBook = new Book("1-1-2-3-5", "Random author", "Title");
      Book txBook = new Book("1-2-2-4-8", "Not so random author", "Title");

      created[0] = bookService.createBookTransactionalCache(nonTxBook);
      found[0] = bookService.findBookTransactionalCache(Integer.valueOf(9));
      created[1] = bookService.createBookNonTransactionalCache(txBook);
      found[1] = bookService.findBookNonTransactionalCache(Integer.valueOf(9));

      // rollback transaction
      tm.rollback();
      // check whether rollback caused cached items to be removed from transactional cache
      assert !transactionalCache().values().contains(created[0]);
      assert !transactionalCache().values().contains(found[0]);
      // rollback should not have any impact on non-transactional cache
      assert nonTransactionalCache().values().contains(created[1]);
      assert nonTransactionalCache().values().contains(found[1]);
      // make sure books have not been persisted
      assert bookService.findBookCacheDisabled(created[0].getId()) == null;
      assert bookService.findBookCacheDisabled(created[1].getId()) == null;
   }

   @Test
   public void testUsingTemplateNoRollback() throws SystemException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
      final Book[] created = new Book[2];
      final Book[] found = new Book[2];

      assert !transactionalCache().containsKey(Integer.valueOf(9));

      TransactionManager tm = getTransactionManager("booksTransactional");
      assert tm.getStatus() == 0 : "Transaction should be in state 'RUNNING'";

      Book nonTxBook = new Book("1-1-2-3-5", "Random author", "Title");
      Book txBook = new Book("1-2-2-4-8", "Not so random author", "Title");
      created[0] = bookService.createBookTransactionalCache(nonTxBook);
      found[0] = bookService.findBookTransactionalCache(Integer.valueOf(9));
      created[1] = bookService.createBookNonTransactionalCache(txBook);
      found[1] = bookService.findBookNonTransactionalCache(Integer.valueOf(9));

      // commit changes
      tm.commit();
      // all items should be cached regardless of cache type
      assert transactionalCache().values().contains(created[0]);
      assert transactionalCache().values().contains(found[0]);
      assert nonTransactionalCache().values().contains(created[1]);
      assert nonTransactionalCache().values().contains(found[1]);
      // check whether books have been persisted
      assert bookService.findBookCacheDisabled(created[0].getId()) != null;
      assert bookService.findBookCacheDisabled(created[1].getId()) != null;
   }

   private TransactionManager getTransactionManager(String cacheName) {
      Cache cache = (Cache) cacheManager.getCache(cacheName).getNativeCache();
      return cache.getAdvancedCache().getTransactionManager();
   }

   private BasicCache<Object, Object> nonTransactionalCache() {
      return (BasicCache<Object, Object>) cacheManager.getCache("books").getNativeCache();
   }

   private BasicCache<Object, Object> transactionalCache() {
      return (BasicCache<Object, Object>) cacheManager.getCache("booksTransactional").getNativeCache();
   }
}

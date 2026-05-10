package org.infinispan.spring.embedded.provider.sample;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.invoke.MethodHandles;
import java.util.Random;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.spring.embedded.provider.sample.entity.Book;
import org.infinispan.spring.embedded.provider.sample.service.CachedBookService;
import org.infinispan.spring.embedded.provider.sample.service.CachedBookServiceImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.testng.AbstractTransactionalTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Abstract template for running a set of tests under different configurations, in order to illustrate how Spring handles
 * the caching aspects we added to {@link CachedBookServiceImpl <code>CachedBookServiceImpl</code>}.
 * It calls each method defined in the class and verifies that book instances are indeed cached and removed from the
 * cache as specified.
 *
 * @author Olaf Bergner
 * @author Marius Bogoevici
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
@Test(groups = "functional")
public abstract class AbstractTestTemplate extends AbstractTransactionalTestNGSpringContextTests {

   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @AfterMethod
   public void clearBookCache() {
      booksCache().clear();
      backupCache().clear();
   }

   /**
    * Demonstrates that loading a {@link Book <code>book</code>} via
    * {@link CachedBookServiceImpl#findBook(Integer)} does indeed cache
    * the returned book instance under the supplied bookId.
    */
   @Test
   public void demonstrateCachingLoadedBooks() {
      final Integer bookToCacheId = 5;

      assertFalse(booksCache().containsKey(bookToCacheId), "Cache should not initially contain the book with id " + bookToCacheId);

      final Book cachedBook = getBookService().findBook(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assertEquals(booksCache().get(bookToCacheId), cachedBook, "findBook(" + bookToCacheId + ") should have cached book");
   }

   /**
    * Demonstrate that removing a {@link Book <code>book</code>} from database via
    * {@link CachedBookServiceImpl#deleteBook(Integer)} does indeed remove
    * it from cache also.
    */
   @Test
   public void demonstrateRemovingBookFromCache() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assertFalse(booksCache().containsKey(bookToDeleteId), "Cache should not initially contain the book with id " + bookToDeleteId);

      final Book bookToDelete = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assertEquals(bookToDelete, booksCache().get(bookToDeleteId), "findBook(" + bookToDeleteId + ") should have cached book");

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBook(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assertFalse(booksCache().containsKey(bookToDeleteId), "deleteBook(" + bookToDelete + ") should have evicted book from cache.");
   }

   /**
    * Demonstrates that updating a {@link Book <code>book</code>} that has already been persisted to
    * database via {@link CachedBookServiceImpl (Book)} does indeed evict
    * that book from cache.
    */
   @Test
   public void demonstrateCacheEvictionUponUpdate() {
      final Integer bookToUpdateId = 2;

      assertFalse(booksCache().containsKey(bookToUpdateId), "Cache should not initially contain the book with id " + bookToUpdateId);

      log.infof("Caching book [ID = %d]", bookToUpdateId);
      final Book bookToUpdate = getBookService().findBook(bookToUpdateId);
      assertEquals(bookToUpdate, booksCache().get(bookToUpdateId), "findBook(" + bookToUpdateId + ") should have cached book");

      log.infof("Updating book [%s] ...", bookToUpdate);
      bookToUpdate.setTitle("Work in Progress");
      getBookService().updateBook(bookToUpdate);
      log.infof("Book [%s] updated", bookToUpdate);

      assertFalse(booksCache().containsKey(bookToUpdateId), "updateBook(" + bookToUpdate + ") should have removed updated book from cache");
   }

   /**
    * Demonstrates that creating a new {@link Book <code>book</code>} via
    * {@link CachedBookServiceImpl#createBook(Book)}
    * does indeed cache returned book under its generated id.
    */
   @Test
   public void demonstrateCachePutOnCreate() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBook(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assertEquals(bookToCreate, booksCache().get(bookToCreate.getId()), "createBook(" + bookToCreate + ") should have inserted created book into cache");
   }

   @Test
   public void testFindCustomCacheResolver() {
      final Integer bookToCacheId = 5;

      assertFalse(getCache("custom").containsKey(bookToCacheId), "Cache should not initially contain the book with id " + bookToCacheId);

      final Book cachedBook = getBookService().findBookCustomCacheResolver(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assertEquals(getCache("custom").get(bookToCacheId), cachedBook, "findBook(" + bookToCacheId + ") should have cached book");
   }

   @Test
   public void testFindCustomKeyGenerator() {
      final Integer bookToCacheId = 5;

      assertFalse(booksCache().containsKey(bookToCacheId), "Cache should not initially contain the book with id " + bookToCacheId);

      final Book cachedBook = getBookService().findBookCustomKeyGenerator(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assertEquals(booksCache().get(bookToCacheId), cachedBook, "findBook(" + bookToCacheId + ") should have cached book");
   }

   @Test
   public void testFindConditionMet() {
      final Integer bookToCacheId = 5;

      assertFalse(booksCache().containsKey(bookToCacheId), "Cache should not initially contain the book with id " + bookToCacheId);

      final Book cachedBook = getBookService().findBookCondition(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assertEquals(booksCache().get(bookToCacheId), cachedBook, "findBook(" + bookToCacheId + ") should have cached book");
   }

   @Test
   public void testFindConditionNotMet() {
      final Integer bookToCacheId = 1;

      assertFalse(booksCache().containsKey(bookToCacheId), "Cache should not initially contain the book with id " + bookToCacheId);

      final Book cachedBook = getBookService().findBookCondition(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assertFalse(booksCache().containsKey(bookToCacheId), "findBook(" + bookToCacheId + ") should not have cached book");
   }

   @Test
   public void testFindUnlessMet() {
      final Integer bookToCacheId = 1;

      assertFalse(booksCache().containsKey(bookToCacheId), "Cache should not initially contain the book with id " + bookToCacheId);

      final Book cachedBook = getBookService().findBookUnless(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assertEquals(booksCache().get(bookToCacheId), cachedBook, "findBook(" + bookToCacheId + ") should have cached book");
   }

   @Test
   public void testFindUnlessNotMet() {
      final Integer bookToCacheId = 5;

      assertFalse(booksCache().containsKey(bookToCacheId), "Cache should not initially contain the book with id " + bookToCacheId);

      final Book cachedBook = getBookService().findBookUnless(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assertFalse(booksCache().containsKey(bookToCacheId), "findBook(" + bookToCacheId + ") should not have cached book");
   }

   @Test
   public void testFindCustomCacheManager() {
      final Integer bookToCacheId = 5;

      assertFalse(booksCache().containsKey(bookToCacheId), "Cache should not initially contain the book with id " + bookToCacheId);

      final Book cachedBook = getBookService().findBookCustomCacheManager(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assertEquals(booksCache().get(bookToCacheId), cachedBook, "findBook(" + bookToCacheId + ") should have cached book");
   }

   @Test
   public void testCreateCustomCacheManager() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookCustomCacheManager(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assertEquals(booksCache().get(bookToCreate.getId()), bookToCreate, "createBook(" + bookToCreate + ") should have inserted created book into cache");
   }

   @Test
   public void testCreateCustomCacheResolver() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookCustomCacheResolver(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assertEquals(getCache("custom").get(bookToCreate.getId()), bookToCreate, "createBook(" + bookToCreate + ") should have inserted created book into cache");
   }

   @Test
   public void testCreateCustomKeyGenerator() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookCustomKeyGenerator(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assertTrue(booksCache().containsKey(bookToCreate), "createBook(" + bookToCreate + ") should have inserted created book into cache");
   }

   @Test
   public void testCreateConditionMet() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      Book result = getBookService().createBookCondition(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assertEquals(booksCache().get(result.getId()), bookToCreate, "createBook(" + bookToCreate + ") should have inserted created book into cache");
   }

   @Test
   public void testCreateConditionNotMet() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Wrong Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookCondition(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assertNotNull(bookToCreate.getId(), "Book.id should have been set.");
      assertFalse(booksCache().containsKey(bookToCreate.getId()), "createBook(" + bookToCreate + ") should not have inserted created book into cache");
   }

   @Test
   public void testCreateUnlessMet() {
      final Book bookToCreate = new Book("99-999-999", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookUnless(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assertEquals(booksCache().get(bookToCreate.getId()), bookToCreate, "createBook(" + bookToCreate + ") should have inserted created book into cache");
   }

   @Test
   public void testCreateUnlessNotMet() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookUnless(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assertFalse(booksCache().containsKey(bookToCreate.getId()), "createBook(" + bookToCreate + ") should not have inserted created book into cache");
   }

   @Test
   public void testDeleteCustomCacheResolver() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assertFalse(getCache("custom").containsKey(bookToDeleteId), "Cache should not initially contain the book with id " + bookToDeleteId);

      final Book bookToDelete = getBookService().findBookCustomCacheResolver(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assertNotNull(bookToDelete.getId(), "Book.id should have been set.");
      assertEquals(getCache("custom").get(bookToDelete.getId()), bookToDelete, "findBook(" + bookToDeleteId + ") should have cached book");

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBookCustomCacheResolver(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assertFalse(getCache("custom").containsKey(bookToDelete.getId()), "deleteBook(" + bookToDelete + ") should have evicted book from cache.");
   }

   @Test
   public void testDeleteCustomKeyGenerator() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assertFalse(booksCache().containsKey(bookToDeleteId), "Cache should not initially contain the book with id " + bookToDeleteId);

      final Book bookToDelete = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assertEquals(booksCache().get(bookToDeleteId), bookToDelete, "findBook(" + bookToDeleteId + ") should have cached book");

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBookCustomKeyGenerator(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assertFalse(booksCache().containsKey(bookToDelete.getId()), "deleteBook(" + bookToDelete + ") should have evicted book from cache.");
   }

   @Test
   public void testDeleteConditionMet() {
      final Integer bookToDeleteId = 2;

      assertFalse(booksCache().containsKey(bookToDeleteId), "Cache should not initially contain the book with id " + bookToDeleteId);

      final Book bookToDelete = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assertEquals(booksCache().get(bookToDeleteId), bookToDelete, "findBook(" + bookToDeleteId + ") should have cached book");

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBookCondition(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assertFalse(booksCache().containsKey(bookToDelete.getId()), "deleteBook(" + bookToDelete + ") should have evicted book from cache.");
   }

   @Test
   public void testDeleteConditionNotMet() {
      final Integer bookToDeleteId = 1;

      assertFalse(booksCache().containsKey(bookToDeleteId), "Cache should not initially contain the book with id " + bookToDeleteId);

      final Book bookToDelete = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assertEquals(booksCache().get(bookToDeleteId), bookToDelete, "findBook(" + bookToDeleteId + ") should have cached book");

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBookCondition(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assertEquals(booksCache().get(bookToDeleteId), bookToDelete, "deleteBook(" + bookToDelete + ") should have evicted book from cache.");
   }

   @Test
   public void testDeleteAllEntries() {
      final Integer bookToDeleteId1 = 5;
      final Integer bookToDeleteId2 = 6;

      assertFalse(booksCache().containsKey(bookToDeleteId1), "Cache should not initially contain the book with id " + bookToDeleteId1);
      assertFalse(booksCache().containsKey(bookToDeleteId2), "Cache should not initially contain the book with id " + bookToDeleteId2);

      final Book bookToDelete1 = getBookService().findBook(bookToDeleteId1);
      log.infof("Book [%s] cached", bookToDelete1);

      assertEquals(booksCache().get(bookToDeleteId1), bookToDelete1, "findBook(" + bookToDeleteId1 + ") should have cached book");

      final Book bookToDelete2 = getBookService().findBook(bookToDeleteId2);
      log.infof("Book [%s] cached", bookToDelete2);

      assertEquals(booksCache().get(bookToDeleteId2), bookToDelete2, "findBook(" + bookToDeleteId2 + ") should have cached book");

      log.infof("Deleting book [%s] ...", bookToDelete1);
      getBookService().deleteBookAllEntries(bookToDeleteId1);
      log.infof("Book [%s] deleted", bookToDelete1);

      assertFalse(booksCache().containsKey(bookToDelete1.getId()), "deleteBook(" + bookToDelete1 + ") should have evicted book from cache.");
      assertFalse(booksCache().containsKey(bookToDelete2.getId()), "deleteBook(" + bookToDelete2 + ") should have evicted book from cache.");
   }

   @Test
   public void testDeleteCustomCacheManager() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assertFalse(booksCache().containsKey(bookToDeleteId), "Cache should not initially contain the book with id " + bookToDeleteId);

      final Book bookToDelete = getBookService().findBookCustomCacheManager(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assertNotNull(bookToDelete.getId(), "Book.id should have been set.");
      assertEquals(booksCache().get(bookToDelete.getId()), bookToDelete, "findBook(" + bookToDeleteId + ") should have cached book");

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBookCustomCacheManager(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assertFalse(booksCache().containsKey(bookToDelete.getId()), "deleteBook(" + bookToDelete + ") should have evicted book from cache.");
   }

   @Test
   public void testDeleteBookBeforeInvocation() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assertFalse(booksCache().containsKey(bookToDeleteId), "Cache should not initially contain the book with id " + bookToDeleteId);

      final Book bookToDelete = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assertEquals(booksCache().get(bookToDelete.getId()), bookToDelete, "findBook(" + bookToDeleteId + ") should have cached book");

      log.infof("Deleting book [%s] ...", bookToDelete);
      try {
         getBookService().deleteBookBeforeInvocation(bookToDeleteId);
      } catch (IllegalStateException e) {
         // ok, expected
      }
      log.infof("Book [%s] deleted", bookToDelete);

      assertFalse(booksCache().containsKey(bookToDelete.getId()), "deleteBook(" + bookToDelete + ") should have evicted book from cache.");
   }

   @Test
   public void testCachingCreate() {
      Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookCachingBackup(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assertEquals(booksCache().get(bookToCreate.getId()), bookToCreate, "createBook(" + bookToCreate + ") should have inserted created book into cache");
      assertEquals(backupCache().get(bookToCreate.getId()), bookToCreate, "createBook(" + bookToCreate + ") should have inserted created book into cache");
   }

   @Test
   public void testCachingFind() {

      final Integer bookToCacheId = 5;

      assertFalse(booksCache().containsKey(bookToCacheId), "Cache should not initially contain the book with id " + bookToCacheId);
      assertFalse(backupCache().containsKey(bookToCacheId), "Cache should not initially contain the book with id " + bookToCacheId);

      final Book cachedBook = getBookService().findBookCachingBackup(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assertEquals(booksCache().get(cachedBook.getId()), cachedBook, "findBook(" + bookToCacheId + ") should have cached book");
      assertEquals(backupCache().get(cachedBook.getId()), cachedBook, "findBook(" + bookToCacheId + ") should have cached book");
   }

   @Test
   public void testCachingDelete() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assertFalse(booksCache().containsKey(bookToDeleteId), "Cache should not initially contain the book with id " + bookToDeleteId);
      assertFalse(backupCache().containsKey(bookToDeleteId), "Cache should not initially contain the book with id " + bookToDeleteId);

      final Book bookToDelete1 = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete1);
      final Book bookToDelete2 = getBookService().findBookBackup(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete2);

      assertEquals(booksCache().get(bookToDeleteId), bookToDelete1, "findBook(" + bookToDeleteId + ") should have cached book");
      assertEquals(backupCache().get(bookToDeleteId), bookToDelete1, "findBook(" + bookToDeleteId + ") should have cached book");

      log.infof("Deleting book [%s] ...", bookToDelete1);
      getBookService().deleteBookCachingBackup(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete1);

      assertFalse(booksCache().containsKey(bookToDelete1.getId()), "deleteBook(" + bookToDelete1 + ") should have evicted book from cache.");
      assertFalse(backupCache().containsKey(bookToDelete1.getId()), "deleteBook(" + bookToDelete2 + ") should have evicted book from cache.");
   }

   protected BasicCache<Object, Object> booksCache() {
      return (BasicCache<Object, Object>) getCacheManager().getCache("books").getNativeCache();
   }

   protected BasicCache<Object, Object> backupCache() {
      return (BasicCache<Object, Object>) getCacheManager().getCache("backup").getNativeCache();
   }

   protected BasicCache<Object, Object> getCache(String name) {
      return (BasicCache<Object, Object>) getCacheManager().getCache(name).getNativeCache();
   }

   public abstract CachedBookService getBookService();

   public abstract CacheManager getCacheManager();
}

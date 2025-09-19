package org.infinispan.spring.embedded.provider.sample;

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

      assert !booksCache().containsKey(bookToCacheId) : "Cache should not initially contain the book with id " + bookToCacheId;

      final Book cachedBook = getBookService().findBook(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assert cachedBook.equals(booksCache().get(bookToCacheId)) : "findBook(" + bookToCacheId
            + ") should have cached book";
   }

   /**
    * Demonstrate that removing a {@link Book <code>book</code>} from database via
    * {@link CachedBookServiceImpl#deleteBook(Integer)} does indeed remove
    * it from cache also.
    */
   @Test
   public void demonstrateRemovingBookFromCache() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assert !booksCache().containsKey(bookToDeleteId) : "Cache should not initially contain the book with id " + bookToDeleteId;

      final Book bookToDelete = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assert booksCache().get(bookToDeleteId).equals(bookToDelete) : "findBook(" + bookToDeleteId
            + ") should have cached book";

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBook(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assert !booksCache().containsKey(bookToDeleteId) : "deleteBook(" + bookToDelete
            + ") should have evicted book from cache.";
   }

   /**
    * Demonstrates that updating a {@link Book <code>book</code>} that has already been persisted to
    * database via {@link CachedBookServiceImpl (Book)} does indeed evict
    * that book from cache.
    */
   @Test
   public void demonstrateCacheEvictionUponUpdate() {
      final Integer bookToUpdateId = 2;

      assert !booksCache().containsKey(bookToUpdateId): "Cache should not initially contain the book with id " + bookToUpdateId;

      log.infof("Caching book [ID = %d]", bookToUpdateId);
      final Book bookToUpdate = getBookService().findBook(bookToUpdateId);
      assert booksCache().get(bookToUpdateId).equals(bookToUpdate) : "findBook(" + bookToUpdateId
            + ") should have cached book";

      log.infof("Updating book [%s] ...", bookToUpdate);
      bookToUpdate.setTitle("Work in Progress");
      getBookService().updateBook(bookToUpdate);
      log.infof("Book [%s] updated", bookToUpdate);

      assert !booksCache().containsKey(bookToUpdateId) : "updateBook(" + bookToUpdate
            + ") should have removed updated book from cache";
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

      assert booksCache().get(bookToCreate.getId()).equals(bookToCreate) : "createBook(" + bookToCreate
            + ") should have inserted created book into cache";
   }

   @Test
   public void testFindCustomCacheResolver() {
      final Integer bookToCacheId = 5;

      assert !getCache("custom").containsKey(bookToCacheId): "Cache should not initially contain the book with id " + bookToCacheId;

      final Book cachedBook = getBookService().findBookCustomCacheResolver(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assert cachedBook.equals(getCache("custom").get(bookToCacheId)) : "findBook(" + bookToCacheId
            + ") should have cached book";
   }

   @Test
   public void testFindCustomKeyGenerator() {
      final Integer bookToCacheId = 5;

      assert !booksCache().containsKey(bookToCacheId): "Cache should not initially contain the book with id " + bookToCacheId;

      final Book cachedBook = getBookService().findBookCustomKeyGenerator(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assert cachedBook.equals(booksCache().get(bookToCacheId)) : "findBook(" + bookToCacheId
            + ") should have cached book";
   }

   @Test
   public void testFindConditionMet() {
      final Integer bookToCacheId = 5;

      assert !booksCache().containsKey(bookToCacheId): "Cache should not initially contain the book with id " + bookToCacheId;

      final Book cachedBook = getBookService().findBookCondition(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assert cachedBook.equals(booksCache().get(bookToCacheId)) : "findBook(" + bookToCacheId
            + ") should have cached book";
   }

   @Test
   public void testFindConditionNotMet() {
      final Integer bookToCacheId = 1;

      assert !booksCache().containsKey(bookToCacheId): "Cache should not initially contain the book with id " + bookToCacheId;

      final Book cachedBook = getBookService().findBookCondition(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assert !booksCache().containsKey(bookToCacheId) : "findBook(" + bookToCacheId
            + ") should not have cached book";
   }

   @Test
   public void testFindUnlessMet() {
      final Integer bookToCacheId = 1;

      assert !booksCache().containsKey(bookToCacheId): "Cache should not initially contain the book with id " + bookToCacheId;

      final Book cachedBook = getBookService().findBookUnless(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assert cachedBook.equals(booksCache().get(bookToCacheId)) : "findBook(" + bookToCacheId
            + ") should have cached book";
   }

   @Test
   public void testFindUnlessNotMet() {
      final Integer bookToCacheId = 5;

      assert !booksCache().containsKey(bookToCacheId): "Cache should not initially contain the book with id " + bookToCacheId;

      final Book cachedBook = getBookService().findBookUnless(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assert !booksCache().containsKey(bookToCacheId) : "findBook(" + bookToCacheId
            + ") should not have cached book";
   }

   @Test
   public void testFindCustomCacheManager() {
      final Integer bookToCacheId = 5;

      assert !booksCache().containsKey(bookToCacheId): "Cache should not initially contain the book with id " + bookToCacheId;

      final Book cachedBook = getBookService().findBookCustomCacheManager(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assert cachedBook.equals(booksCache().get(bookToCacheId)) : "findBook(" + bookToCacheId
            + ") should have cached book";
   }

   @Test
   public void testCreateCustomCacheManager() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookCustomCacheManager(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assert bookToCreate.equals(booksCache().get(bookToCreate.getId())) : "createBook(" + bookToCreate
            + ") should have inserted created book into cache";
   }

   @Test
   public void testCreateCustomCacheResolver() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookCustomCacheResolver(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assert bookToCreate.equals(getCache("custom").get(bookToCreate.getId())) : "createBook(" + bookToCreate
            + ") should have inserted created book into cache";
   }

   @Test
   public void testCreateCustomKeyGenerator() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookCustomKeyGenerator(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assert booksCache().containsKey(bookToCreate) : "createBook(" + bookToCreate
            + ") should have inserted created book into cache";
   }

   @Test
   public void testCreateConditionMet() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      Book result = getBookService().createBookCondition(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assert bookToCreate.equals(booksCache().get(result.getId())) : "createBook(" + bookToCreate
            + ") should have inserted created book into cache";
   }

   @Test
   public void testCreateConditionNotMet() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Wrong Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookCondition(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assert bookToCreate.getId() != null : "Book.id should have been set.";
      assert !booksCache().containsKey(bookToCreate.getId()) : "createBook(" + bookToCreate
            + ") should not have inserted created book into cache";
   }

   @Test
   public void testCreateUnlessMet() {
      final Book bookToCreate = new Book("99-999-999", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookUnless(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assert bookToCreate.equals(booksCache().get(bookToCreate.getId())) : "createBook(" + bookToCreate
            + ") should have inserted created book into cache";
   }

   @Test
   public void testCreateUnlessNotMet() {
      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookUnless(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assert !booksCache().containsKey(bookToCreate.getId()) : "createBook(" + bookToCreate
            + ") should not have inserted created book into cache";
   }

   @Test
   public void testDeleteCustomCacheResolver() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assert !getCache("custom").containsKey(bookToDeleteId): "Cache should not initially contain the book with id " + bookToDeleteId;

      final Book bookToDelete = getBookService().findBookCustomCacheResolver(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assert bookToDelete.getId() != null : "Book.id should have been set.";
      assert bookToDelete.equals(getCache("custom").get(bookToDelete.getId())) : "findBook(" + bookToDeleteId
            + ") should have cached book";

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBookCustomCacheResolver(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assert !getCache("custom").containsKey(bookToDelete.getId()) : "deleteBook(" + bookToDelete
            + ") should have evicted book from cache.";
   }

   @Test
   public void testDeleteCustomKeyGenerator() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assert !booksCache().containsKey(bookToDeleteId): "Cache should not initially contain the book with id " + bookToDeleteId;

      final Book bookToDelete = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assert bookToDelete.equals(booksCache().get(bookToDeleteId)) : "findBook(" + bookToDeleteId
            + ") should have cached book";

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBookCustomKeyGenerator(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assert !booksCache().containsKey(bookToDelete.getId()) : "deleteBook(" + bookToDelete
            + ") should have evicted book from cache.";
   }

   @Test
   public void testDeleteConditionMet() {
      final Integer bookToDeleteId = 2;

      assert !booksCache().containsKey(bookToDeleteId): "Cache should not initially contain the book with id " + bookToDeleteId;

      final Book bookToDelete = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assert bookToDelete.equals(booksCache().get(bookToDeleteId)) : "findBook(" + bookToDeleteId
            + ") should have cached book";

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBookCondition(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assert !booksCache().containsKey(bookToDelete.getId()) : "deleteBook(" + bookToDelete
            + ") should have evicted book from cache.";
   }

   @Test
   public void testDeleteConditionNotMet() {
      final Integer bookToDeleteId = 1;

      assert !booksCache().containsKey(bookToDeleteId): "Cache should not initially contain the book with id " + bookToDeleteId;

      final Book bookToDelete = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assert bookToDelete.equals(booksCache().get(bookToDeleteId)) : "findBook(" + bookToDeleteId
            + ") should have cached book";

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBookCondition(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assert bookToDelete.equals(booksCache().get(bookToDeleteId)) : "deleteBook(" + bookToDelete
            + ") should have evicted book from cache.";
   }

   @Test
   public void testDeleteAllEntries() {
      final Integer bookToDeleteId1 = 5;
      final Integer bookToDeleteId2 = 6;

      assert !booksCache().containsKey(bookToDeleteId1): "Cache should not initially contain the book with id " + bookToDeleteId1;
      assert !booksCache().containsKey(bookToDeleteId2): "Cache should not initially contain the book with id " + bookToDeleteId2;

      final Book bookToDelete1 = getBookService().findBook(bookToDeleteId1);
      log.infof("Book [%s] cached", bookToDelete1);

      assert bookToDelete1.equals(booksCache().get(bookToDeleteId1)) : "findBook(" + bookToDeleteId1
            + ") should have cached book";

      final Book bookToDelete2 = getBookService().findBook(bookToDeleteId2);
      log.infof("Book [%s] cached", bookToDelete2);

      assert bookToDelete2.equals(booksCache().get(bookToDeleteId2)) : "findBook(" + bookToDeleteId2
            + ") should have cached book";

      log.infof("Deleting book [%s] ...", bookToDelete1);
      getBookService().deleteBookAllEntries(bookToDeleteId1);
      log.infof("Book [%s] deleted", bookToDelete1);

      assert !booksCache().containsKey(bookToDelete1.getId()) : "deleteBook(" + bookToDelete1
            + ") should have evicted book from cache.";
      assert !booksCache().containsKey(bookToDelete2.getId()) : "deleteBook(" + bookToDelete2
            + ") should have evicted book from cache.";
   }

   @Test
   public void testDeleteCustomCacheManager() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assert !booksCache().containsKey(bookToDeleteId): "Cache should not initially contain the book with id " + bookToDeleteId;

      final Book bookToDelete = getBookService().findBookCustomCacheManager(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assert bookToDelete.getId() != null : "Book.id should have been set.";
      assert bookToDelete.equals(booksCache().get(bookToDelete.getId())) : "findBook(" + bookToDeleteId
            + ") should have cached book";

      log.infof("Deleting book [%s] ...", bookToDelete);
      getBookService().deleteBookCustomCacheManager(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete);

      assert !booksCache().containsKey(bookToDelete.getId()) : "deleteBook(" + bookToDelete
            + ") should have evicted book from cache.";
   }

   @Test
   public void testDeleteBookBeforeInvocation() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assert !booksCache().containsKey(bookToDeleteId): "Cache should not initially contain the book with id " + bookToDeleteId;

      final Book bookToDelete = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete);

      assert bookToDelete.equals(booksCache().get(bookToDelete.getId())) : "findBook(" + bookToDeleteId
            + ") should have cached book";

      log.infof("Deleting book [%s] ...", bookToDelete);
      try {
         getBookService().deleteBookBeforeInvocation(bookToDeleteId);
      } catch (IllegalStateException e) {
         // ok, expected
      }
      log.infof("Book [%s] deleted", bookToDelete);

      assert !booksCache().containsKey(bookToDelete.getId()) : "deleteBook(" + bookToDelete
            + ") should have evicted book from cache.";
   }

   @Test
   public void testCachingCreate() {
      Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      log.infof("Creating book [%s] ...", bookToCreate);
      getBookService().createBookCachingBackup(bookToCreate);
      log.infof("Book [%s] created", bookToCreate);

      assert bookToCreate.equals(booksCache().get(bookToCreate.getId())) : "createBook(" + bookToCreate
            + ") should have inserted created book into cache";
      assert bookToCreate.equals(backupCache().get(bookToCreate.getId())) : "createBook(" + bookToCreate
            + ") should have inserted created book into cache";
   }

   @Test
   public void testCachingFind() {

      final Integer bookToCacheId = 5;

      assert !booksCache().containsKey(bookToCacheId) : "Cache should not initially contain the book with id " + bookToCacheId;
      assert !backupCache().containsKey(bookToCacheId) : "Cache should not initially contain the book with id " + bookToCacheId;

      final Book cachedBook = getBookService().findBookCachingBackup(bookToCacheId);
      log.infof("Book [%s] cached", cachedBook);

      assert cachedBook.equals(booksCache().get(cachedBook.getId())) : "findBook(" + bookToCacheId
            + ") should have cached book";
      assert cachedBook.equals(backupCache().get(cachedBook.getId())) : "findBook(" + bookToCacheId
            + ") should have cached book";
   }

   @Test
   public void testCachingDelete() {
      final Integer bookToDeleteId = new Random().nextInt(10) + 1;

      assert !booksCache().containsKey(bookToDeleteId) : "Cache should not initially contain the book with id " + bookToDeleteId;
      assert !backupCache().containsKey(bookToDeleteId) : "Cache should not initially contain the book with id " + bookToDeleteId;

      final Book bookToDelete1 = getBookService().findBook(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete1);
      final Book bookToDelete2 = getBookService().findBookBackup(bookToDeleteId);
      log.infof("Book [%s] cached", bookToDelete2);

      assert bookToDelete1.equals(booksCache().get(bookToDeleteId)) : "findBook(" + bookToDeleteId
            + ") should have cached book";
      assert bookToDelete1.equals(backupCache().get(bookToDeleteId)) : "findBook(" + bookToDeleteId
            + ") should have cached book";

      log.infof("Deleting book [%s] ...", bookToDelete1);
      getBookService().deleteBookCachingBackup(bookToDeleteId);
      log.infof("Book [%s] deleted", bookToDelete1);

      assert !booksCache().containsKey(bookToDelete1.getId()) : "deleteBook(" + bookToDelete1
            + ") should have evicted book from cache.";
      assert !backupCache().containsKey(bookToDelete1.getId()) : "deleteBook(" + bookToDelete2
            + ") should have evicted book from cache.";
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

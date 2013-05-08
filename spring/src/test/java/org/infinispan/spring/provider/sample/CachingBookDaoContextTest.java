package org.infinispan.spring.provider.sample;

import java.util.Random;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.spring.provider.SpringEmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * <p>
 * A test that tries to illustrate how Spring handles the caching aspects we added to
 * {@link JdbcBookDao <code>JdbcBookDao</code>}. It calls each method defined on {@link BookDao
 * <code>BookDao</code>} and verifies that book instances are indeed cached and removed from the
 * cache as specified.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 * @since 5.1
 */
@Test(testName = "spring.provider.CachingBookDaoContextTest", groups = "integration")
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/provider/sample/CachingBookDaoContextTest.xml")
public class CachingBookDaoContextTest extends AbstractTestNGSpringContextTests {

   private final Log log = LogFactory.getLog(getClass());

   @Autowired(required = true)
   private BookDao bookDao;

   @Autowired(required = true)
   private SpringEmbeddedCacheManager booksCacheManager;

   @BeforeClass
   public void installCacheListener() {
      booksCache().addListener(this.new LoggingListener());
   }

   @AfterMethod
   public void clearBookCache() {
      booksCache().clear();
   }

   @Listener
   public class LoggingListener {

      @CacheEntryCreated
      public void onCacheEntryCreated(CacheEntryCreatedEvent<Object, Object> cacheEntryCreated) {
         if (!cacheEntryCreated.isPre())
            log.infof("Object cached: [Key = %s | Event = %s]", cacheEntryCreated.getKey(),
                     cacheEntryCreated);
      }

      @CacheEntryRemoved
      public void onCacheEntryRemoved(CacheEntryRemovedEvent<Object, Object> cacheEntryRemoved) {
         if (cacheEntryRemoved.isPre())
            log.infof("Object removed from cache: [Key = %s | Event = %s]",
                     cacheEntryRemoved.getKey(), cacheEntryRemoved);
      }
   }

   /**
    * Demonstrates that loading a {@link Book <code>book</code>} via
    * {@link JdbcBookDao#findBook(Integer)} does indeed cache the returned book instance under the
    * supplied bookId.
    * 
    */
   @Test
   public void demonstrateCachingLoadedBooks() {
      final Integer bookToCacheId = Integer.valueOf(5);

      assert booksCache().isEmpty() : "Cache should initially not contain any books";

      final Book cachedBook = this.bookDao.findBook(bookToCacheId);
      this.log.infof("Book [%s] cached", cachedBook);

      assert booksCache().values().contains(cachedBook) : "findBook(" + bookToCacheId
               + ") should have cached book";
   }

   /**
    * Demonstrate that removing a {@link Book <code>book</code>} from database via
    * {@link JdbcBookDao#deleteBook(Integer)} does indeed remove it from cache also.
    * 
    */
   @Test
   public void demonstrateRemovingBookFromCache() {
      final Integer bookToDeleteId = Integer.valueOf(new Random().nextInt(10) + 1);

      assert booksCache().isEmpty() : "Cache should initially not contain any books";

      final Book bookToDelete = this.bookDao.findBook(bookToDeleteId);
      this.log.infof("Book [%s] cached", bookToDelete);

      assert booksCache().values().contains(bookToDelete) : "findBook(" + bookToDeleteId
               + ") should have cached book";

      this.log.infof("Deleting book [%s] ...", bookToDelete);
      this.bookDao.deleteBook(bookToDeleteId);
      this.log.infof("Book [%s] deleted", bookToDelete);

      assert !booksCache().values().contains(bookToDelete) : "deleteBook(" + bookToDelete
               + ") should have evicted book from cache.";
   }

   /**
    * Demonstrates that updating a {@link Book <code>book</code>} that has already been persisted to
    * database via {@link JdbcBookDao#storeBook(Book)} does indeed evict that book from cache.
    * 
    */
   @Test
   public void demonstrateCacheEvictionUponUpdate() {
      final Integer bookToUpdateId = Integer.valueOf(2);

      assert booksCache().isEmpty() : "Cache should initially not contain any books";

      this.log.infof("Caching book [ID = %d]", bookToUpdateId);
      final Book bookToUpdate = this.bookDao.findBook(bookToUpdateId);
      assert booksCache().values().contains(bookToUpdate) : "findBook(" + bookToUpdateId
               + ") should have cached book";

      this.log.infof("Updating book [%s] ...", bookToUpdate);
      bookToUpdate.setTitle("Work in Progress");
      this.bookDao.storeBook(bookToUpdate);
      this.log.infof("Book [%s] updated", bookToUpdate);

      assert !booksCache().values().contains(bookToUpdate) : "storeBook(" + bookToUpdate
               + ") should have removed updated book from cache";
   }

   private Cache<?,?> booksCache() {
      return (Cache)this.booksCacheManager.getCache("books").getNativeCache();
   }
}

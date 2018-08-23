package org.infinispan.spring.remote.provider.sample;

import java.util.Random;

import org.infinispan.spring.remote.provider.sample.entity.Book;
import org.infinispan.spring.remote.provider.sample.service.CachedBookServiceJsr107;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

/**
 * Abstract template for running a set of tests under different configurations, including JSR-107 annotations.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
public abstract class AbstractTestTemplateJsr107 extends AbstractTestTemplate {

   @Autowired
   private CachedBookServiceJsr107 bookDao;

   @Test
   public void demonstrateCachingLoadedBooksJsr107() {
      final Integer bookToCacheId = Integer.valueOf(5);

      assert !booksCache().containsKey(bookToCacheId) : "Cache should not initially contain the book with id " + bookToCacheId;

      final Book cachedBook = this.bookDao.findBook(bookToCacheId);
      this.log.infof("Book [%s] cached", cachedBook);

      assert cachedBook.equals(booksCache().get(bookToCacheId)) : "findBook(" + bookToCacheId
            + ") should have cached book";
   }

   @Test
   public void demonstrateRemovingBookFromCacheJsr107() {
      final Integer bookToDeleteId = Integer.valueOf(new Random().nextInt(10) + 1);

      assert !booksCache().containsKey(bookToDeleteId) : "Cache should not initially contain the book with id " + bookToDeleteId;

      final Book bookToDelete = bookDao.findBook(bookToDeleteId);
      this.log.infof("Book [%s] cached", bookToDelete);

      assert booksCache().get(bookToDeleteId).equals(bookToDelete) : "findBook(" + bookToDeleteId
            + ") should have cached book";

      this.log.infof("Deleting book [%s] ...", bookToDelete);
      bookDao.deleteBook(bookToDeleteId);
      this.log.infof("Book [%s] deleted", bookToDelete);

      assert !booksCache().containsKey(bookToDeleteId) : "deleteBook(" + bookToDelete
            + ") should have evicted book from cache.";
   }

   @Test
   public void demonstrateCacheEvictionUponUpdateJsr107() {
      final Integer bookToUpdateId = Integer.valueOf(2);

      assert !booksCache().containsKey(bookToUpdateId): "Cache should not initially contain the book with id " + bookToUpdateId;

      Book bookToUpdate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");
      booksCache().put(bookToUpdate, bookToUpdate);

      assert booksCache().containsKey(bookToUpdate);

      bookToUpdate.setTitle("Work in Progress");
      Book result = bookDao.updateBook(bookToUpdate);
      this.log.infof("Book [%s] updated", bookToUpdate);

      assert !booksCache().containsKey(bookToUpdate) : "updateBook(" + bookToUpdate
            + ") should have removed updated book from cache";
   }

   @Test
   public void demonstrateCachePutOnCreateJsr107() {

      final Book bookToCreate = new Book("112-358-132", "Random Author", "Path to Infinispan Enlightenment");

      this.log.infof("Creating book [%s] ...", bookToCreate);
      Book result = bookDao.createBook(bookToCreate);
      this.log.infof("Book [%s] created", bookToCreate);

      assert booksCache().get(result).equals(bookToCreate) : "createBook(" + bookToCreate
            + ") should have inserted created book into cache";
   }

   @Test
   public void testDeleteAllEntriesJsr107() {
      final Integer bookToDeleteId1 = Integer.valueOf(5);
      final Integer bookToDeleteId2 = Integer.valueOf(6);

      assert !booksCache().containsKey(bookToDeleteId1): "Cache should not initially contain the book with id " + bookToDeleteId1;
      assert !booksCache().containsKey(bookToDeleteId2): "Cache should not initially contain the book with id " + bookToDeleteId2;

      final Book bookToDelete1 = bookDao.findBook(bookToDeleteId1);
      this.log.infof("Book [%s] cached", bookToDelete1);

      assert bookToDelete1.equals(booksCache().get(bookToDeleteId1)) : "findBook(" + bookToDeleteId1
            + ") should have cached book";

      final Book bookToDelete2 = bookDao.findBook(bookToDeleteId2);
      this.log.infof("Book [%s] cached", bookToDelete2);

      assert bookToDelete2.equals(booksCache().get(bookToDeleteId2)) : "findBook(" + bookToDeleteId2
            + ") should have cached book";

      this.log.infof("Deleting book [%s] ...", bookToDelete1);
      bookDao.deleteBookAllEntries(bookToDeleteId1);
      this.log.infof("Book [%s] deleted", bookToDelete1);

      assert !booksCache().containsKey(bookToDelete1.getId()) : "deleteBook(" + bookToDelete1
            + ") should have evicted book from cache.";
      assert !booksCache().containsKey(bookToDelete2.getId()) : "deleteBook(" + bookToDelete2
            + ") should have evicted book from cache.";
   }
}

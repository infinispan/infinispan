package org.infinispan.spring.provider.sample.dao;

import org.infinispan.spring.provider.sample.entity.Book;

/**
 * A simple, woefully incomplete {@code DAO} for storing, retrieving and removing {@link org.infinispan.spring.provider.sample.entity.Book
 * <code>Books</code>}.
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @since 5.1
 */
public interface BaseBookDao {

   /**
    * <p>
    * Look up and return the {@code Book} identified by the supplied {@code bookId}, or {@code null}
    * if no such book exists.
    * </p>
    *
    * @param bookId
    * @return The {@code Book} identified by the supplied {@code bookId}, or {@code null}
    */
   Book findBook(Integer bookId);

   /**
    * <p>
    * Remove the {@code Book} identified by the supplied {@code bookId} from this store.
    * </p>
    *
    * @param bookId
    */
   void deleteBook(Integer bookId);

   /**
    * <p>
    * Update provided {@code book} and return its updated version.
    * </p>
    *
    * @param book
    *           The book to update
    * @return Updated book
    */
   Book updateBook(Book book);

   /**
    * <p>
    * Create new book and return it. If the book already exists, throw exception.
    * </p>
    *
    * @param book The book to create
    * @return Created book.
    */
   Book createBook(Book book);
}

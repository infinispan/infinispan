package org.infinispan.spring.embedded.provider.sample.dao;

import org.infinispan.spring.embedded.provider.sample.entity.Book;

/**
 * <p>
 * A simple, woefully incomplete {@code DAO} for storing, retrieving and removing {@link Book
 * <code>Books</code>}.
 * </p>
 *
 * @author Olaf Bergner
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

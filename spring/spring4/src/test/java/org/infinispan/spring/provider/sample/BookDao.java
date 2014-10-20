package org.infinispan.spring.provider.sample;

/**
 * <p>
 * A simple, woefully incomplete {@code DAO} for storing, retrieving and removing {@link Book
 * <code>Books</code>}.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @since 5.1
 */
public interface BookDao {

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
    * Store the provided {@code book}. Depending on whether {@code book} has already been store
    * before this method will either perform an {@code insert} or an {@code update}. Return the
    * stored book.
    * </p>
    *
    * @param book
    *           The book to store
    * @return The stored book
    */
   Book storeBook(Book book);
}

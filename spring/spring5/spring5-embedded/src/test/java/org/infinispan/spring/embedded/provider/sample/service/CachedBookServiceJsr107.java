package org.infinispan.spring.embedded.provider.sample.service;

import org.infinispan.spring.embedded.provider.sample.entity.Book;

/**
 * Service providing basic CRUD operations in order to test individual JSR-107 caching annotations.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
public interface CachedBookServiceJsr107 {

   Book findBook(Integer bookId);

   Book updateBook(Book book);

   Book createBook(Book book);

   void deleteBook(Integer bookId);

   void deleteBookAllEntries(Integer bookId);
}

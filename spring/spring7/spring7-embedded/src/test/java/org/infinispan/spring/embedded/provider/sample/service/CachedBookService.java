package org.infinispan.spring.embedded.provider.sample.service;

import org.infinispan.spring.embedded.provider.sample.entity.Book;

/**
 * Service providing extension to basic CRUD operations in order to test individual caching annotations and parameters
 * they support.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
public interface CachedBookService {

   Book findBook(Integer bookId);

   Book findBookCondition(Integer bookId);

   Book findBookUnless(Integer bookId);

   Book findBookCustomKeyGenerator(Integer bookId);

   Book findBookCustomCacheResolver(Integer bookId);

   Book findBookBackup(Integer bookId);

   Book findBookCachingBackup(Integer bookId);

   Book findBookCustomCacheManager(Integer bookId);

   Book createBook(Book book);

   Book createBookCondition(Book book);

   Book createBookUnless(Book book);

   Book createBookCustomKeyGenerator(Book book);

   Book createBookCustomCacheResolver(Book book);

   Book createBookCustomCacheManager(Book book);

   Book createBookCachingBackup(Book book);

   void deleteBook(Integer bookId);

   void deleteBookCondition(Integer bookId);

   void deleteBookCustomKeyGenerator(Integer bookId);

   void deleteBookCustomCacheResolver(Integer bookId);

   void deleteBookAllEntries(Integer bookId);

   void deleteBookCachingBackup(Integer bookId);

   void deleteBookCustomCacheManager(Integer bookId);

   void deleteBookBeforeInvocation(Integer bookId);

   Book updateBook(Book book);
}

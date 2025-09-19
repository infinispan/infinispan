package org.infinispan.spring.remote.provider.sample.service;

import org.infinispan.spring.remote.provider.sample.entity.Book;

/**
 * Service providing basic create/find operations on both transactional and non-transactional caches.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
public interface CachedTransactionBookService {

   public Book createBookNonTransactionalCache(Book book);

   public Book createBookTransactionalCache(Book book);

   public Book findBookNonTransactionalCache(Integer id);

   public Book findBookTransactionalCache(Integer id);

   public Book findBookCacheDisabled(Integer id);
}

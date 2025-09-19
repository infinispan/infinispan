package org.infinispan.spring.embedded.provider.sample.service;

import org.infinispan.spring.embedded.provider.sample.entity.Book;

/**
 * Service providing basic create/find operations on both transactional and non-transactional caches.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
public interface CachedTransactionBookService {

   Book createBookNonTransactionalCache(Book book);

   Book createBookTransactionalCache(Book book);

   Book findBookNonTransactionalCache(Integer id);

   Book findBookTransactionalCache(Integer id);

   Book findBookCacheDisabled(Integer id);
}

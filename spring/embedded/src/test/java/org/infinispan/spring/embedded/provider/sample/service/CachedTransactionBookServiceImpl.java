package org.infinispan.spring.embedded.provider.sample.service;

import org.infinispan.spring.embedded.provider.sample.dao.BaseBookDao;
import org.infinispan.spring.embedded.provider.sample.entity.Book;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
@Transactional
@Service
public class CachedTransactionBookServiceImpl implements CachedTransactionBookService {

   @Autowired
   private BaseBookDao baseDao;

   @CachePut(value = "books", key = "#book.id")
   @Override
   public Book createBookNonTransactionalCache(Book book) {
      return baseDao.createBook(book);
   }

   @CachePut(value = "booksTransactional", key = "#book.id")
   @Override
   public Book createBookTransactionalCache(Book book) {
      return baseDao.createBook(book);
   }

   @Cacheable(value = "books")
   @Override
   public Book findBookNonTransactionalCache(Integer id) {
      return baseDao.findBook(id);
   }

   @Cacheable(value = "booksTransactional")
   @Override
   public Book findBookTransactionalCache(Integer id) {
      return baseDao.findBook(id);
   }

   @Override
   public Book findBookCacheDisabled(Integer id) {
      return baseDao.findBook(id);
   }
}

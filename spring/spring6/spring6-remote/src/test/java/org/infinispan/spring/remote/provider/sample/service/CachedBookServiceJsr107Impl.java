package org.infinispan.spring.remote.provider.sample.service;

import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheRemove;
import javax.cache.annotation.CacheRemoveAll;
import javax.cache.annotation.CacheResult;
import javax.cache.annotation.CacheValue;

import org.infinispan.spring.remote.provider.sample.dao.BaseBookDao;
import org.infinispan.spring.remote.provider.sample.entity.Book;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
@Transactional
@Service
public class CachedBookServiceJsr107Impl implements CachedBookServiceJsr107 {

   @Autowired
   private BaseBookDao baseDao;

   @CacheResult(cacheName = "books")
   @Override
   public Book findBook(Integer bookId) {
      return baseDao.findBook(bookId);
   }

   @CacheRemove(cacheName = "books")
   @Override
   public void deleteBook(Integer bookId) {
      baseDao.deleteBook(bookId);
   }

   @CacheRemove(cacheName = "books")
   @Override
   public Book updateBook(Book book) {
      return baseDao.updateBook(book);
   }

   @CachePut(cacheName = "books")
   @Override
   public Book createBook(@CacheKey @CacheValue Book book) {
      return baseDao.createBook(book);
   }

   @CacheRemoveAll(cacheName = "books")
   @Override
   public void deleteBookAllEntries(Integer bookId) {
      baseDao.deleteBook(bookId);
   }
}

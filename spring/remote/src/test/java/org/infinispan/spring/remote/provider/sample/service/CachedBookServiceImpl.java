package org.infinispan.spring.remote.provider.sample.service;

import org.infinispan.spring.remote.provider.sample.dao.JdbcBookDao;
import org.infinispan.spring.remote.provider.sample.entity.Book;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Olaf Bergner
 * @author Matej Cimbora (mcimbora@redhat.com)
 */

@Transactional
@Service
public class CachedBookServiceImpl implements CachedBookService {

   @Autowired
   private JdbcBookDao baseDao;

   /**
    * <p>
    * Look up and return the {@code Book} identified by the supplied {@code bookId}. By annotating
    * this method with {@code @Cacheable(value = "books", key = "#bookId")} we achieve the
    * following:
    * <ol>
    * <li>
    * {@code Book} instances returned from this method will be cached in a named
    * {@link org.springframework.cache.Cache <code>Cache</code>} &quot;books&quot;</li>
    * <li>
    * The key used to cache {@code Book} instances will be the supplied {@code bookId}.</li>
    * </ol>
    * </p>
    * <p>
    * Note that it is <b>important</b> that we explicitly tell Spring to use {@code bookId}
    * as the cache key. Otherwise, Spring would <b>derive</b> a cache key from the
    * parameters passed in (in our case only {@code bookId}), a cache key we have no control over.
    * This would get us into trouble when in {@link #updateBook(Book)} we need a book's cache key to
    * remove it from the cache. But we wouldn't know that cache key since we don't know Spring's key
    * generation algorithm. Therefore, we consistently use {@code key = "#bookId"} or
    * {@code key = "#book.id"} to tell Spring to <b>always</b> use a book's id as its
    * cache key.
    * </p>
    */
   @Override
   @Cacheable(value = "books", key = "#bookId")
   public Book findBook(Integer bookId) {
      return baseDao.findBook(bookId);
   }

   /**
    * <p>
    * Remove the book identified by the supplied {@code bookId} from database. By annotating this
    * method with {@code @CacheEvict(value = "books", key = "#bookId")} we make sure that Spring
    * will remove the book cache under key {@code bookId} (if any) from the
    * {@link org.springframework.cache.Cache <code>Cache</code>} &quot;books&quot;.
    * </p>
    */
   @Override
   @CacheEvict(value = "books", key = "#bookId")
   public void deleteBook(Integer bookId) {
      baseDao.deleteBook(bookId);
   }

   /**
    * <p>
    * Store the supplied {@code bookToStore} in database. Since it is annotated with
    * {@code @CacheEvict(value = "books", key = "#book.id", condition = "#book.id != null")}
    * this method will tell Spring to remove any book cached under the key
    * {@code book.getId()} from the {@link org.springframework.cache.Cache
    * <code>Cache</code>} &quot;books&quot;. This eviction will only be triggered if that id is not
    * {@code null}.
    * </p>
    */
   @Override
   @CacheEvict(value = "books", key = "#book.id", condition = "#book.id != null")
   public Book updateBook(Book book) {
      return baseDao.updateBook(book);
   }

   @Override
   @CachePut(value = "books", key = "#book.id")
   public Book createBook(Book book) {
      return baseDao.createBook(book);
   }

   @Override
   @Cacheable(value = "books", key = "#bookId", condition = "#bookId > 1")
   public Book findBookCondition(Integer bookId) {
      return baseDao.findBook(bookId);
   }

   @Override
   @Cacheable(value = "books", key = "#bookId", unless = "#bookId > 1")
   public Book findBookUnless(Integer bookId) {
      return baseDao.findBook(bookId);
   }

   @Override
   @Cacheable(value = "books", keyGenerator = "singleArgKeyGenerator")
   public Book findBookCustomKeyGenerator(Integer bookId) {
      return baseDao.findBook(bookId);
   }

   @Override
   @Cacheable(value = "books", key = "#bookId", cacheResolver = "customCacheResolver")
   public Book findBookCustomCacheResolver(Integer bookId) {
      return baseDao.findBook(bookId);
   }

   @Override
   @Cacheable(value = "backup", key = "#bookId")
   public Book findBookBackup(Integer bookId) {
      return baseDao.findBook(bookId);
   }

   @Override
   @CachePut(value = "books", key = "#book.id", condition = "#book.title == 'Path to Infinispan Enlightenment'")
   public Book createBookCondition(Book book) {
      return baseDao.createBook(book);
   }

   @Override
   @CachePut(value = "books", key = "#book.id", unless = "#book.isbn == '112-358-132'")
   public Book createBookUnless(Book book) {
      return baseDao.createBook(book);
   }

   @Override
   @CachePut(value = "books", keyGenerator = "singleArgKeyGenerator")
   public Book createBookCustomKeyGenerator(Book book) {
      return baseDao.createBook(book);
   }

   @Override
   @CachePut(value = "books", key = "#book.id", cacheResolver = "customCacheResolver")
   public Book createBookCustomCacheResolver(Book book) {
      return baseDao.createBook(book);
   }

   @Override
   @CachePut(value = "books", key = "#book.id", cacheManager = "cacheManager")
   public Book createBookCustomCacheManager(Book book) {
      return baseDao.createBook(book);
   }

   @Override
   @CacheEvict(value = "books", key = "#bookId", condition = "#bookId > 1")
   public void deleteBookCondition(Integer bookId) {
      baseDao.deleteBook(bookId);
   }

   @Override
   @CacheEvict(value = "books", keyGenerator = "singleArgKeyGenerator")
   public void deleteBookCustomKeyGenerator(Integer bookId) {
      baseDao.deleteBook(bookId);
   }

   @Override
   @CacheEvict(value = "books", key = "#bookId", cacheResolver = "customCacheResolver")
   public void deleteBookCustomCacheResolver(Integer bookId) {
      baseDao.deleteBook(bookId);
   }

   @Override
   @CacheEvict(value = "books", key = "#bookId", allEntries = true)
   public void deleteBookAllEntries(Integer bookId) {
      baseDao.deleteBook(bookId);
   }

   @Override
   @Caching(put = {@CachePut(value = "books", key = "#book.id"), @CachePut(value = "backup", key = "#book.id")})
   public Book createBookCachingBackup(Book book) {
      return baseDao.createBook(book);
   }

   @Override
   @Caching(cacheable = {@Cacheable(value = "books", key = "#bookId"), @Cacheable(value = "backup", key = "#bookId")})
   public Book findBookCachingBackup(Integer bookId) {
      return baseDao.findBook(bookId);
   }

   @Override
   @Cacheable(value = "books", cacheManager = "cacheManager")
   public Book findBookCustomCacheManager(Integer bookId) {
      return baseDao.findBook(bookId);
   }

   @Override
   @Caching(evict = {@CacheEvict(value = "books"), @CacheEvict(value = "backup")})
   public void deleteBookCachingBackup(Integer bookId) {
      baseDao.deleteBook(bookId);
   }

   @Override
   @CacheEvict(value = "books", cacheManager = "cacheManager")
   public void deleteBookCustomCacheManager(Integer bookId) {
      baseDao.deleteBook(bookId);
   }

   @Override
   @CacheEvict(value = "books", beforeInvocation = true)
   public void deleteBookBeforeInvocation(Integer bookId) {
      throw new IllegalStateException("This method throws exception by default.");
   }
}

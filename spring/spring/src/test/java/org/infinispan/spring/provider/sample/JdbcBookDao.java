package org.infinispan.spring.provider.sample;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>
 * {@link org.infinispan.spring.provider.sample.BookDao <code>BookDao</code>} implementation that fronts a relational database, using
 * {@code JDBC} to store and retrieve {@link Book <code>books</code>}. Serves as an example of how
 * to use <a href="http://www.springframework.org">Spring</a>'s
 * {@link org.springframework.cache.annotation.Cacheable <code>@Cacheable</code>} and
 * {@link org.springframework.cache.annotation.CacheEvict <code>@CacheEvict</code>}.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @since 5.1
 */
@Repository
public class JdbcBookDao implements BookDao {

   private final Log log = LogFactory.getLog(getClass());

   private SimpleJdbcTemplate jdbcTemplate;

   private SimpleJdbcInsert bookInsert;

   @Autowired(required = true)
   public void initialize(final DataSource dataSource) {
      this.jdbcTemplate = new SimpleJdbcTemplate(dataSource);
      this.bookInsert = new SimpleJdbcInsert(dataSource).withTableName("books")
            .usingGeneratedKeyColumns("id");
   }

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
    * Note that it is <strong>important</strong> that we explicitly tell Spring to use {@code bookId}
    * as the cache key. Otherwise, Spring would <strong>derive</strong> a cache key from the
    * parameters passed in (in our case only {@code bookId}), a cache key we have no control over.
    * This would get us into trouble when in {@link #storeBook(Book)} we need a book's cache key to
    * remove it from the cache. But we wouldn't know that cache key since we don't know Spring's key
    * generation algorithm. Therefore, we consistently use {@code key = "#bookId"} or
    * {@code key = "#book.id"} to tell Spring to <strong>always</strong> use a book's id as its
    * cache key.
    * </p>
    */
   @Transactional
   @Cacheable(value = "books", key = "#bookId")
   @Override
   public Book findBook(Integer bookId) {
      try {
         this.log.infof("Loading book [ID = %d]", bookId);
         return this.jdbcTemplate.queryForObject("SELECT * FROM books WHERE id = ?",
                                                 new BookRowMapper(), bookId);
      } catch (EmptyResultDataAccessException e) {
         return null;
      }
   }

   private static final class BookRowMapper implements RowMapper<Book> {

      @Override
      public Book mapRow(ResultSet rs, int rowNum) throws SQLException {
         final Book mappedBook = new Book();
         mappedBook.setId(rs.getInt("id"));
         mappedBook.setIsbn(rs.getString("isbn"));
         mappedBook.setAuthor(rs.getString("author"));
         mappedBook.setTitle(rs.getString("title"));

         return mappedBook;
      }
   }

   /**
    * <p>
    * Remove the book identified by the supplied {@code bookId} from database. By annotating this
    * method with {@code @CacheEvict(value = "books", key = "#bookId")} we make sure that Spring
    * will remove the book cache under key {@code bookId} (if any) from the
    * {@link org.springframework.cache.Cache <code>Cache</code>} &quot;books&quot;.
    * </p>
    */
   @Transactional
   @CacheEvict(value = "books", key = "#bookId")
   @Override
   public void deleteBook(Integer bookId) {
      this.log.infof("Deleting book [ID = %d]", bookId);
      this.jdbcTemplate.update("DELETE FROM books WHERE id = ?", bookId);
   }

   /**
    * <p>
    * Store the supplied {@code bookToStore} in database. Since it is annotated with
    * {@code @CacheEvict(value = "books", key = "#bookToStore.id", condition = "#bookToStore.id != null")}
    * this method will tell Spring to remove any book cached under the key
    * {@code bookToStore.getId()} from the {@link org.springframework.cache.Cache
    * <code>Cache</code>} &quot;books&quot;. This eviction will only be triggered if that id is not
    * {@code null}.
    * </p>
    */
   @Transactional
   @CacheEvict(value = "books", key = "#bookToStore.id", condition = "#bookToStore.id != null")
   @Override
   public Book storeBook(Book bookToStore) {
      this.log.infof("Storing book [%s]", bookToStore);
      if (bookToStore.getId() == null) {
         final Number newBookId = this.bookInsert
               .executeAndReturnKey(createParameterSourceFor(bookToStore));
         bookToStore.setId(newBookId.intValue());
         this.log.infof("Book [%s] persisted for the first time", bookToStore);
      } else {
         this.jdbcTemplate.update(
               "UPDATE books SET isbn = :isbn, author = :author, title = :title WHERE id = :id",
               createParameterSourceFor(bookToStore));
         this.log.infof("Book [%s] updated", bookToStore);
      }
      return bookToStore;
   }

   private SqlParameterSource createParameterSourceFor(final Book book) {
      return new MapSqlParameterSource().addValue("id", book.getId())
            .addValue("isbn", book.getIsbn()).addValue("author", book.getAuthor())
            .addValue("title", book.getTitle());
   }
}

package org.infinispan.spring.embedded.provider.sample.dao;

import java.lang.invoke.MethodHandles;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import javax.sql.DataSource;

import org.infinispan.spring.embedded.provider.sample.entity.Book;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Repository;

/**
 * <p>
 * {@link BaseBookDao <code>BookDao</code>} implementation that fronts a relational database, using
 * {@code JDBC} to store and retrieve {@link Book <code>books</code>}. Serves as an example of how
 * to use <a href="http://www.springframework.org">Spring</a>'s
 * {@link org.springframework.cache.annotation.Cacheable <code>@Cacheable</code>} and
 * {@link org.springframework.cache.annotation.CacheEvict <code>@CacheEvict</code>}.
 * </p>
 *
 * @author Olaf Bergner
 * @since 5.1
 */
@Repository(value = "jdbcBookDao")
public class JdbcBookDao implements BaseBookDao {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private NamedParameterJdbcTemplate jdbcTemplate;

   private SimpleJdbcInsert bookInsert;

   @Autowired(required = true)
   public void initialize(final DataSource dataSource) {
      this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
      this.bookInsert = new SimpleJdbcInsert(dataSource).withTableName("books")
            .usingGeneratedKeyColumns("id");
   }

   public Book findBook(Integer bookId) {
      try {
         log.infof("Loading book [ID = %d]", bookId);
         return this.jdbcTemplate.queryForObject("SELECT * FROM books WHERE id = :id",
               new MapSqlParameterSource("id", bookId), new BookRowMapper());
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

   @Override
   public void deleteBook(Integer bookId) {
      log.infof("Deleting book [ID = %d]", bookId);
      this.jdbcTemplate.update("DELETE FROM books WHERE id = :id", new MapSqlParameterSource("id", bookId));
   }

   public Collection<Book> getBooks() {
      return this.jdbcTemplate.query("SELECT * FROM books", new BookRowMapper());
   }

   @Override
   public Book updateBook(Book bookToUpdate) {
      log.infof("Updating book [%s]", bookToUpdate);
      this.jdbcTemplate.update(
            "UPDATE books SET isbn = :isbn WHERE id = :id",
            createParameterSourceFor(bookToUpdate));
      log.infof("Book [%s] updated", bookToUpdate);
      return bookToUpdate;
   }

   @Override
   public Book createBook(Book bookToCreate) {
      final Number newBookId = this.bookInsert
            .executeAndReturnKey(createParameterSourceFor(bookToCreate));
      bookToCreate.setId(newBookId.intValue());
      log.infof("Book [%s] persisted for the first time", bookToCreate);
      return bookToCreate;
   }

   private SqlParameterSource createParameterSourceFor(final Book book) {
      return new MapSqlParameterSource().addValue("id", book.getId())
            .addValue("isbn", book.getIsbn()).addValue("author", book.getAuthor())
            .addValue("title", book.getTitle());
   }
}

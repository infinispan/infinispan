package org.infinispan.spring.provider.sample.dao;

import org.infinispan.spring.provider.sample.entity.Book;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

/**
 * <p>
 * {@link org.infinispan.spring.provider.sample.dao.BaseBookDao <code>BookDao</code>} implementation that fronts a relational database, using
 * {@code JDBC} to store and retrieve {@link org.infinispan.spring.provider.sample.entity.Book <code>books</code>}. Serves as an example of how
 * to use <a href="http://www.springframework.org">Spring</a>'s
 * {@link org.springframework.cache.annotation.Cacheable <code>@Cacheable</code>} and
 * {@link org.springframework.cache.annotation.CacheEvict <code>@CacheEvict</code>}.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @since 5.1
 */
@Repository(value = "jdbcBookDao")
public class JdbcBookDao implements BaseBookDao {

   private final Log log = LogFactory.getLog(getClass());

   private SimpleJdbcTemplate jdbcTemplate;

   private SimpleJdbcInsert bookInsert;

   @Autowired(required = true)
   public void initialize(final DataSource dataSource) {
      this.jdbcTemplate = new SimpleJdbcTemplate(dataSource);
      this.bookInsert = new SimpleJdbcInsert(dataSource).withTableName("books")
            .usingGeneratedKeyColumns("id");
   }

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

   @Override
   public void deleteBook(Integer bookId) {
      this.log.infof("Deleting book [ID = %d]", bookId);
      this.jdbcTemplate.update("DELETE FROM books WHERE id = ?", bookId);
   }

   public Collection<Book> getBooks() {
      return this.jdbcTemplate.query("SELECT * FROM books", new BookRowMapper());
   }

   @Override
   public Book updateBook(Book bookToUpdate) {
      this.log.infof("Updating book [%s]", bookToUpdate);
      this.jdbcTemplate.update(
            "UPDATE books SET isbn = :isbn, author = :author, title = :title WHERE id = :id",
            createParameterSourceFor(bookToUpdate));
      this.log.infof("Book [%s] updated", bookToUpdate);
      return bookToUpdate;
   }

   @Override
   public Book createBook(Book bookToCreate) {
      final Number newBookId = this.bookInsert
            .executeAndReturnKey(createParameterSourceFor(bookToCreate));
      bookToCreate.setId(newBookId.intValue());
      this.log.infof("Book [%s] persisted for the first time", bookToCreate);
      return bookToCreate;
   }

   private SqlParameterSource createParameterSourceFor(final Book book) {
      return new MapSqlParameterSource().addValue("id", book.getId())
            .addValue("isbn", book.getIsbn()).addValue("author", book.getAuthor())
            .addValue("title", book.getTitle());
   }
}

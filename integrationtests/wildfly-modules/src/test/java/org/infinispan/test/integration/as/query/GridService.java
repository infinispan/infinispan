package org.infinispan.test.integration.as.query;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;

import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

@Named
@ApplicationScoped
public class GridService {

   @Inject
   private Cache<String, Book> bookshelf;

   public void store(String isbn, Book book, boolean index) {
      if (index) {
         bookshelf.put(isbn, book);
      }
      else {
         bookshelf.getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(isbn, book);
      }
   }

   public Book findById(String isbn) {
      return bookshelf.get(isbn);
   }

   public List<Book> findFullText(String phrase) {
      QueryFactory queryFactory = Search.getQueryFactory(bookshelf);
      Query query = queryFactory.create(String.format("FROM %s where title:'%s'", Book.class.getName(), phrase));
      return query.list();
   }

   public List<Book> findByPublisher(String publisher) {
      Query query = Search.getQueryFactory(bookshelf)
            .from(Book.class)
            .having("publisher")
            .eq(publisher)
            .build();
      return query.list();
   }

   public void clear() {
      bookshelf.clear();
   }

   public void rebuildIndexes() {
      Search.getSearchManager(bookshelf).getMassIndexer().start();
   }

}

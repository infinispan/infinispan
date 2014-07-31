package org.infinispan.test.integration.as.query;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;

@Named
@ApplicationScoped
public class GridService {

   @Inject
   private Cache<String,Book> bookshelf;

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

   public List<Object> findFullText(String phrase) {
      SearchManager sm = Search.getSearchManager(bookshelf);
      QueryBuilder queryBuilder = sm.buildQueryBuilderForClass(Book.class).get();
      Query query = queryBuilder
               .phrase()
               .onField("title")
               .sentence(phrase)
               .createQuery();
      CacheQuery cacheQuery = sm.getQuery(query);
      return cacheQuery.list();
   }

   public void clear() {
      bookshelf.clear();
   }

   public void rebuildIndexes() {
      Search.getSearchManager(bookshelf).getMassIndexer().start();
   }

}

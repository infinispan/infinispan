package org.infinispan.distexec.mapreduce;

import java.util.Iterator;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Example for simple Map Reduce use case.
 * The test is marked as abstract for applying different configurations on it.
 *
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "distexec.BookSearchTest")
public abstract class BookSearchTest extends MultipleCacheManagersTest {

   @SuppressWarnings({ "rawtypes", "unchecked" })
   public void testBookSearch() {
      Cache c1 = cache(0, "bookSearch");
      c1.put("1",
               new Book("Seam in Action",
                        "Dan Allen",
                        "Manning"));
      c1.put("2",
               new Book("Hibernate Search in Action",
                        "Emmanuel Bernard and John Griffin",
                        "Manning"));
      c1.put("3",
               new Book("Metaprogramming Ruby",
                        "Paolo Perrotta",
                        "The Pragmatic Programmers"));
      for (int i = 0; i < 4; i++) {
         verifySearch( cache( i,  "bookSearch" ) );
      }
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })   
   private void verifySearch(Cache cache) {
      
      MapReduceTask<String, Book, String, Book> queryTask = new MapReduceTask<String, Book, String, Book>(cache);

      queryTask
         .mappedWith( new TitleBookSearcher( "Hibernate Search in Action" ) )
         .reducedWith( new BookReducer() );

      Map<String, Book> queryResult = queryTask.execute();
      assert queryResult.size() == 1;
      assert "Hibernate Search in Action".equals( queryResult.values().iterator().next().title );
   }

   static class TitleBookSearcher implements Mapper<String, Book, String, Book> {

      /** The serialVersionUID */
      private static final long serialVersionUID = -7443288752468217500L;
      final String title;

      public TitleBookSearcher(String title) {
         this.title = title;
      }

      @Override
      public void map(String key, Book value, Collector<String, Book> collector) {
         if ( title.equals( value.title ) ) {
            collector.emit( key, value );
         }
      }
   }

   static class BookReducer implements Reducer<String, Book> {

      /** The serialVersionUID */
      private static final long serialVersionUID = 5686049814166522660L;

      @Override
      public Book reduce(String reducedKey, Iterator<Book> iter) {
         return iter.next();
      }

   }

}

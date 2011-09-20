/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.distexec.mapreduce;

import java.util.Iterator;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Example for simple Map Reduce use case.
 *
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "distexec.BookSearchTest")
public class BookSearchTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      createClusteredCaches(4, cfg);
   }

   public void testBookSearch() {
      Cache c1 = cache(0);
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
         verifySearch( cache( i ) );
      }
   }

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

      @Override
      public Book reduce(String reducedKey, Iterator<Book> iter) {
         return iter.next();
      }

   }

}

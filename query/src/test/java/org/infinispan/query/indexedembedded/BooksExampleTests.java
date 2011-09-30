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

package org.infinispan.query.indexedembedded;

import java.util.List;

import org.apache.lucene.search.Query;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.BooksExampleTests")
public class BooksExampleTests extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration c = getDefaultStandaloneConfig(true);
      c.fluent()
         .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
         .indexing()
         .indexLocalOnly(false)
         .addProperty("hibernate.search.default.directory_provider", "ram")
         .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(c);
   }

   @Test
   public void searchOnEmptyIndex() {
      cache.put("1",
            new Book("Seam in Action",
                     "Dan Allen",
                     "Manning"));
      cache.put("2",
            new Book("Hibernate Search in Action",
                     "Emmanuel Bernard and John Griffin",
                     "Manning"));
      cache.put("3",
            new Book("Megaprogramming Ruby",
                     "Paolo Perrotta",
                     "The Pragmatic Programmers"));

      SearchManager qf = Search.getSearchManager(cache);

      Query query = qf.buildQueryBuilderForClass(Book.class)
         .get()
            .phrase()
               .onField("title")
               .sentence("in action")
            .createQuery();

      List<Object> list = qf.getQuery(query).list();
      assert list.size() == 2;
   }

}

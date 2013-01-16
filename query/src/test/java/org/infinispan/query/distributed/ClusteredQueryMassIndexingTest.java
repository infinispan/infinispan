/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.query.distributed;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.queries.faceting.Car;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;

/**
 * Tests verifying that the Mass Indexing works for Clustered queries as well.
 *
 * @TODO enable the test when ISPN-2661 is fixed.
 */
@Test(groups = "functional", testName = "query.distributed.ClusteredQueryMassIndexingTest", enabled = false)
public class ClusteredQueryMassIndexingTest extends DistributedMassIndexingTest {

   protected void verifyFindsCar(Cache cache, int expectedCount, String carMake) {
      QueryParser queryParser = createQueryParser("make");

      try {
         Query luceneQuery = queryParser.parse(carMake);
         CacheQuery cacheQuery = Search.getSearchManager(cache).getClusteredQuery(luceneQuery, Car.class);

         Assert.assertEquals(expectedCount, cacheQuery.getResultSize());
      } catch(ParseException ex) {
         ex.printStackTrace();
         Assert.fail("Failed due to: " + ex.getMessage());
      }
   }

   //@TODO remove when ISPN-2661 is fixed.
   @Test(enabled = false)
   public void testReindexing() throws Exception {}
}
/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
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
package org.infinispan.query.distributed;

import org.infinispan.context.Flag;
import org.infinispan.query.api.NotIndexedType;
import org.infinispan.query.queries.faceting.Car;
import org.testng.annotations.Test;

/**
 * Running mass indexer on big bunch of data.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.MassIndexingTest")
public class MassIndexingTest extends DistributedMassIndexingTest {

   public void testReindexing() throws Exception {
      for(int i = 0; i < 2000; i++) {
         caches.get(i % 2).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("F" + i + "NUM"),
                                                                                new Car((i % 2 == 0 ? "megane" : "bmw"), "blue", 300 + i));
      }

      //Adding also non-indexed values
      caches.get(0).getAdvancedCache().put(key("FNonIndexed1NUM"), new NotIndexedType("test1"));
      caches.get(0).getAdvancedCache().put(key("FNonIndexed2NUM"), new NotIndexedType("test2"));

      verifyFindsCar(0, "megane");
      verifyFindsCar(0, "test1");
      verifyFindsCar(0, "test2");

      caches.get(0).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("FNonIndexed3NUM"), new NotIndexedType("test3"));
      verifyFindsCar(0, "test3");

      //re-sync datacontainer with indexes:
      rebuildIndexes();

      verifyFindsCar(1000, "megane");
      verifyFindsCar(0, "test1");
      verifyFindsCar(0, "test2");
   }
}

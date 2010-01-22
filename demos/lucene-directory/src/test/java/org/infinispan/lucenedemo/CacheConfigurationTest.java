/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.lucenedemo;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.lucenedemo.DirectoryFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * CacheCreationTest.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test
public class CacheConfigurationTest {
   
   private InfinispanDirectory cacheForIndex1;
   private InfinispanDirectory cacheForIndex2;
   private InfinispanDirectory cacheForIndex3;

   @BeforeClass
   public void init() {
      cacheForIndex1 = DirectoryFactory.getIndex("firstIndex");
      cacheForIndex2 = DirectoryFactory.getIndex("firstIndex");
      cacheForIndex3 = DirectoryFactory.getIndex("secondIndex");
   }
   
   @AfterClass
   public void cleanup() {
      DirectoryFactory.close();
   }

   @Test
   public void testCorrectCacheInstances() {
      assert cacheForIndex1 != null;
      assert cacheForIndex1 == cacheForIndex2;
      assert cacheForIndex1 != cacheForIndex3;
   }
   
   @Test
   public void inserting() throws IOException, ParseException {
      DemoActions node1 = new DemoActions(cacheForIndex1);
      DemoActions node2 = new DemoActions(cacheForIndex2);
      node1.addNewDocument("hello?");
      assert node1.listAllDocuments().size()==1;
      node1.addNewDocument("anybody there?");
      assert node2.listAllDocuments().size()==2;
      Query query = node1.parseQuery("hello world");
      List<String> valuesMatchingQuery = node2.listStoredValuesMatchingQuery(query);
      assert valuesMatchingQuery.size()==1;
      assert valuesMatchingQuery.get(0).equals("hello?");
   }

}

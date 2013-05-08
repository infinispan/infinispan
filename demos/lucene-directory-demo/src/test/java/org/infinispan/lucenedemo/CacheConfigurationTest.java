/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.lucenedemo;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * CacheCreationTest.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "functional", testName = "lucenedemo.CacheConfigurationTest")
public class CacheConfigurationTest {
   
   private EmbeddedCacheManager cacheManager1;
   private EmbeddedCacheManager cacheManager2;
   private InfinispanDirectory directoryNodeOne;
   private InfinispanDirectory directoryNodeTwo;
   private Cache cache1;
   private Cache cache2;

   @BeforeTest
   public void init() throws IOException {
      cacheManager1 = TestCacheManagerFactory.fromXml("config-samples/lucene-demo-cache-config.xml");
      cacheManager1.start();
      cache1 = cacheManager1.getCache();
      cache1.clear();
      directoryNodeOne = new InfinispanDirectory(cache1);
      cacheManager2 = TestCacheManagerFactory.fromXml("config-samples/lucene-demo-cache-config.xml");
      cacheManager2.start();
      cache2 = cacheManager2.getCache();
      cache2.clear();
      directoryNodeTwo = new InfinispanDirectory(cache2);
   }
   
   @AfterTest
   public void cleanup() {
      directoryNodeOne.close();
      directoryNodeTwo.close();
      cacheManager1.stop();
      cacheManager2.stop();
   }

   @Test
   public void inserting() throws IOException, ParseException {
      DemoActions node1 = new DemoActions(directoryNodeOne, cache1);
      DemoActions node2 = new DemoActions(directoryNodeTwo, cache2);
      node1.addNewDocument("hello?");
      assert node1.listAllDocuments().size() == 1;
      node1.addNewDocument("anybody there?");
      assert node2.listAllDocuments().size() == 2;
      Query query = node1.parseQuery("hello world");
      List<String> valuesMatchingQuery = node2.listStoredValuesMatchingQuery(query);
      assert valuesMatchingQuery.size() == 1;
      assert valuesMatchingQuery.get(0).equals("hello?");
   }

}

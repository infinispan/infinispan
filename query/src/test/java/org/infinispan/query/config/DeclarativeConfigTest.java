/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.query.config;

import org.apache.lucene.queryParser.ParseException;
import org.hibernate.search.engine.spi.EntityIndexBinder;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.hibernate.search.store.impl.RAMDirectoryProvider;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertNotNull;

@Test(testName = "query.config.DeclarativeConfigTest", groups = "functional")
public class DeclarativeConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      String config = TestingUtil.INFINISPAN_START_TAG + "\n" +
            "   <default>\n" +
            "      <indexing enabled=\"true\" indexLocalOnly=\"true\">\n" +
            "         <properties>\n" +
            "            <property name=\"default.directory_provider\" value=\"ram\" />\n" +
            "            <property name=\"lucene_version\" value=\"LUCENE_CURRENT\" />\n" +
            "         </properties>\n" +
            "      </indexing>\n" +
            "   </default>\n" + TestingUtil.INFINISPAN_END_TAG;
      System.out.println("Using test configuration:\n\n" + config + "\n");
      InputStream is = new ByteArrayInputStream(config.getBytes());
      try {
         cacheManager = TestCacheManagerFactory.fromStream(is);
      }
      finally {
         is.close();
      }
      cache = cacheManager.getCache();
      return cacheManager;
   }

   public void simpleIndexTest() throws ParseException {
      cache.put("1", new Person("A Person's Name", "A paragraph containing some text", 75));
      CacheQuery cq = TestQueryHelperFactory.createCacheQuery(cache, "name", "Name");
      assertEquals(1, cq.getResultSize());
      List<Object> l =  cq.list();
      assertEquals(1, l.size());
      Person p = (Person) l.get(0);
      assertEquals("A Person's Name", p.getName());
      assertEquals("A paragraph containing some text", p.getBlurb());
      assertEquals(75, p.getAge());
   }

   @Test(dependsOnMethods="simpleIndexTest") //depends as otherwise the Person index is not initialized yet
   public void testPropertiesWhereRead() {
      SearchFactoryIntegrator searchFactory = TestQueryHelperFactory.extractSearchFactory(cache);
      EntityIndexBinder indexBindingForEntity = searchFactory.getIndexBindingForEntity(Person.class);
      IndexManager[] managers = indexBindingForEntity.getIndexManagers();
      assertEquals(1, managers.length);
      assertNotNull(managers[0]);
      assertTrue(managers[0] instanceof DirectoryBasedIndexManager);
      DirectoryBasedIndexManager dbim = (DirectoryBasedIndexManager) managers[0];
      assertTrue(dbim.getDirectoryProvider() instanceof RAMDirectoryProvider);
   }

}

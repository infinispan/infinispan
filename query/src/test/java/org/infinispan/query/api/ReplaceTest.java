/* 
 * Hibernate, Relational Persistence for Idiomatic Java
 * 
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
package org.infinispan.query.api;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;
import static junit.framework.Assert.assertEquals;

@Test(groups = "functional", testName = "query.api.ReplaceTest")
public class ReplaceTest extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .enable()
            .indexLocalOnly(false)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Test
   public void testReplaceSimple() {
      //for comparison we use a non-indexing cache here:
      EmbeddedCacheManager simpleCacheManager = TestCacheManagerFactory.createCacheManager(getDefaultStandaloneConfig(true));
      try {
         Cache<Object, Object> simpleCache = simpleCacheManager.getCache();
         TestEntity se1 = new TestEntity("name1", "surname1", 10, "note");
         TestEntity se2 = new TestEntity("name2", "surname2", 10, "note"); // same id
         simpleCache.put(se1.getId(), se1);
         TestEntity se1ret = (TestEntity) simpleCache.replace(se2.getId(), se2);
         assertEquals(se1, se1ret);
      }
      finally {
         TestingUtil.killCacheManagers(simpleCacheManager);
      }
   }

   @Test
   public void testReplaceSimpleSearchable() {
      TestEntity se1 = new TestEntity("name1", "surname1", 10, "note");
      TestEntity se2 = new TestEntity("name2", "surname2", 10, "note"); // same id
      cache.put(se1.getId(), se1);
      TestEntity se1ret = (TestEntity) cache.replace(se2.getId(), se2);
      assertEquals(se1, se1ret);
   }

   @Test
   public void testReplaceSimpleSearchableConditional() {
      TestEntity se1 = new TestEntity("name1", "surname1", 10, "note");
      TestEntity se2 = new TestEntity("name2", "surname2", 10, "note"); // same id
      cache.put(se1.getId(), se1);
      // note we use conditional replace here
      assert cache.replace(se2.getId(), se1, se2);
   }

}

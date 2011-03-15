/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.infinispan.query.backend;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * Test class for the {@link org.infinispan.query.backend.QueryHelper}
 *
 * @author Navin Surtani
 * @since 4.0
 */

@Test(groups = "unit")
public class QueryHelperTest {
   Configuration cfg;
   List<EmbeddedCacheManager> cacheContainers;

   @BeforeMethod
   public void setUp() {
      cfg = new Configuration();
      cfg.configureIndexing().enabled(true).indexLocalOnly(true);
      cacheContainers = new LinkedList<EmbeddedCacheManager>();
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheContainers);
   }

   private Cache<?, ?> createCache(Configuration c) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c);
      cacheContainers.add(cm);
      return cm.getCache();
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testConstructorWithNoClasses() {
      Cache<?, ?> c = createCache(cfg);
      Class[] classes = new Class[0];
      new QueryHelper(c, null, classes);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testCheckInterceptorChainWithIndexLocalTrue() {
      Cache<?, ?> c = createCache(cfg);
      new QueryHelper(c, null, Person.class);
      new QueryHelper(c, null, Person.class);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testCheckInterceptorChainWithIndexLocalFalse() {
      cfg.configureIndexing().indexLocalOnly(false);
      Cache<?, ?> c = createCache(cfg);
      new QueryHelper(c, null, Person.class);
      new QueryHelper(c, null, Person.class);
   }

   public void testTwoQueryHelpersWithTwoCaches() {
      Cache c1 = createCache(cfg);
      Cache c2 = createCache(cfg);
      new QueryHelper(c1, null, Person.class);
      new QueryHelper(c2, null, Person.class);
   }

}

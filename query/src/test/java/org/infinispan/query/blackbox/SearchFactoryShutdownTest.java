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
package org.infinispan.query.blackbox;

import org.hibernate.search.engine.SearchFactoryImplementor;
import org.hibernate.search.impl.SearchFactoryImpl;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.backend.QueryHelper;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.infinispan.config.Configuration.CacheMode.LOCAL;

/**
 * Ensures the search factory is properly shut down.
 *
 * @author Manik Surtani
 * @version 4.2
 */
@Test(testName = "query.blackbox.SearchFactoryShutdownTest", groups = "functional")
public class SearchFactoryShutdownTest extends AbstractInfinispanTest {
   public void testCorrectShutdown() throws NoSuchFieldException, IllegalAccessException {
      CacheContainer cc = null;

      try {
         Configuration c = SingleCacheManagerTest.getDefaultClusteredConfig(LOCAL, true);
         c.setIndexingEnabled(true);
         c.setIndexLocalOnly(false);
         cc = TestCacheManagerFactory.createCacheManager(c, true);
         Cache<?, ?> cache = cc.getCache();
         QueryHelper qh = TestQueryHelperFactory.createTestQueryHelperInstance(cache, Person.class, AnotherGrassEater.class);
         SearchFactoryImpl sfi = (SearchFactoryImpl) TestingUtil.extractComponent(cache, SearchFactoryImplementor.class);

         assert !isStopped(sfi);

         TestingUtil.killCacheManagers(cc);

         assert isStopped(sfi);
      } finally {
         // proper cleanup for exceptional execution
         TestingUtil.killCacheManagers(cc);
      }
   }

   private boolean isStopped(SearchFactoryImpl sfi) {
      // this sucks - there is no public API to test the Search Factory's status!!!
      // This method may fail if used with future versions of Hibernate Search.

      try {
         Field status = SearchFactoryImpl.class.getDeclaredField("stopped");
         status.setAccessible(true); // to allow access to a private field
         AtomicBoolean b = (AtomicBoolean) status.get(sfi);
         return b.get();
      } catch (Exception e) {
         throw new RuntimeException("Cannot test running state of the search factory", e);
      }

   }
}

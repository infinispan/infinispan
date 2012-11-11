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

import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Ensures the search factory is properly shut down.
 *
 * @author Manik Surtani
 * @version 4.2
 */
@Test(testName = "query.blackbox.SearchFactoryShutdownTest", groups = "functional")
public class SearchFactoryShutdownTest extends AbstractInfinispanTest {
   
   public void testCorrectShutdown() {
      CacheContainer cc = null;

      try {
         ConfigurationBuilder cfg = new ConfigurationBuilder();
         cfg
            .transaction()
               .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing()
               .enable()
               .indexLocalOnly(false)
               .addProperty("default.directory_provider", "ram")
               .addProperty("lucene_version", "LUCENE_CURRENT");
         cc = TestCacheManagerFactory.createCacheManager(cfg);
         Cache<?, ?> cache = cc.getCache();
         SearchFactoryIntegrator sfi = TestingUtil.extractComponent(cache, SearchFactoryIntegrator.class);

         assert ! sfi.isStopped();

         cc.stop();

         assert sfi.isStopped();
      } finally {
         // proper cleanup for exceptional execution
         TestingUtil.killCacheManagers(cc);
      }
   }

}

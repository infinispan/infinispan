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
package org.infinispan.query.config;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.config.CacheModeTest")
public class CacheModeTest extends AbstractInfinispanTest {

   public void testLocal() {
      doTest(CacheMode.LOCAL);
   }

   public void testReplicated() {
      doTest(CacheMode.REPL_SYNC);
   }

   public void testInvalidated() {
      doTest(CacheMode.INVALIDATION_SYNC);
   }

   public void testDistributed() {
      doTest(CacheMode.DIST_SYNC);
   }

   private void doTest(CacheMode m) {
      CacheContainer cc = null;
      try {
         cc = TestCacheManagerFactory.createCacheManager(m, true);
         boolean found = false;
         for (CommandInterceptor i : cc.getCache().getAdvancedCache().getInterceptorChain()) {
            if (i instanceof QueryInterceptor) found = true;
         }
         assert found : "Didn't find a query interceptor in the chain!!";
      } finally {
         TestingUtil.killCacheManagers(cc);
      }
   }
}

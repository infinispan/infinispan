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
package org.infinispan.remoting;

import org.infinispan.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.NamedCacheNotFoundException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import static org.junit.Assert.*;

@Test (testName = "remoting.NonExistentCacheTest", groups = "functional")
public class NonExistentCacheTest extends AbstractInfinispanTest {

   public void testStrictPeerToPeer() {
      doTest(true);
   }

   public void testNonStrictPeerToPeer() {
      doTest(false);
   }

   private EmbeddedCacheManager createCacheManager(boolean strictPeerToPeer) {
      ConfigurationBuilder c = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      c.clustering().cacheMode(CacheMode.REPL_SYNC)
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gc.transport().strictPeerToPeer(strictPeerToPeer);

      return TestCacheManagerFactory.createClusteredCacheManager(gc, c);
   }

   private void doTest(boolean strict) {
      EmbeddedCacheManager cm1 = null, cm2 = null;
      try {
         cm1 = createCacheManager(strict);
         cm2 = createCacheManager(strict);

         cm1.getCache();
         cm2.getCache();

         cm1.getCache().put("k", "v");
         assertEquals("v", cm1.getCache().get("k"));
         assertEquals("v", cm2.getCache().get("k"));

         cm1.defineConfiguration("newCache", cm1.getDefaultCacheConfiguration());

         if (strict) {
            try {
               cm1.getCache("newCache").put("k", "v");
               fail("Should have failed!");
            } catch (CacheException e) {
               assertTrue(e.getCause() instanceof NamedCacheNotFoundException);
            }
         } else {
            cm1.getCache("newCache").put("k", "v");
            assertEquals("v", cm1.getCache("newCache").get("k"));
         }
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

}

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

import org.infinispan.api.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.NamedCacheNotFoundException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

@Test (testName = "remoting.NonExistentCacheTest", groups = "functional")
public class NonExistentCacheTest extends AbstractInfinispanTest {

   public void testStrictPeerToPeer() {
      doTest(true);
   }

   public void testNonStrictPeerToPeer() {
      doTest(false);
   }

   private void doTest(boolean strict) {
      EmbeddedCacheManager cm1 = null, cm2 = null;
      try {
         Configuration c = new Configuration();
         c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
         c.fluent().transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
         GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
         gc.setStrictPeerToPeer(strict);

         cm1 = TestCacheManagerFactory.createCacheManager(gc, c);
         cm2 = TestCacheManagerFactory.createCacheManager(gc, c);

         cm1.getCache();
         cm2.getCache();

         cm1.getCache().put("k", "v");
         assert "v".equals(cm1.getCache().get("k"));
         assert "v".equals(cm2.getCache().get("k"));

         cm1.defineConfiguration("newCache", c);

         if (strict) {
            try {
               cm1.getCache("newCache").put("k", "v");
               assert false : "Should have failed!";
            } catch (CacheException e) {
               assert e.getCause() instanceof NamedCacheNotFoundException;
            }
         } else {
            cm1.getCache("newCache").put("k", "v");
            assert "v".equals(cm1.getCache("newCache").get("k"));
         }
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

}

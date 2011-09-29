/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.config;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "config.TxManagerLookupConfigTest")
public class TxManagerLookupConfigTest {

   static TmA tma = new TmA();
   static TmB tmb = new TmB();

   public void simpleTest() {
      final DefaultCacheManager cacheManager = new DefaultCacheManager(GlobalConfiguration.getNonClusteredDefault(), new Configuration(), true);

      Configuration customConfiguration = TestCacheManagerFactory.getDefaultConfiguration(true);
      customConfiguration.setTransactionManagerLookup(new TxManagerLookupA());
      Configuration definedConfiguration = cacheManager.defineConfiguration("aCache", customConfiguration);

      // verify the setting was not lost:
      assertTrue(definedConfiguration.getTransactionManagerLookup() instanceof TxManagerLookupA);

      // verify it's actually being used:
      TransactionManager activeTransactionManager = cacheManager.getCache("aCache").getAdvancedCache().getTransactionManager();
      assertNotNull(activeTransactionManager);
      assertTrue(activeTransactionManager instanceof TmA);
   }

   public static class TmA extends DummyTransactionManager {}

   public static class TmB extends DummyTransactionManager {}

   public static class TxManagerLookupA implements TransactionManagerLookup {

      @Override
      public TransactionManager getTransactionManager() throws Exception {
         return tma;
      }
   }

   public static class TxManagerLookupB implements TransactionManagerLookup {

      @Override
      public TransactionManager getTransactionManager() throws Exception {
         return tmb;
      }
   }
}

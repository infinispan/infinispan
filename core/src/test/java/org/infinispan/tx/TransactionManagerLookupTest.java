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
package org.infinispan.tx;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

/**
 * Tests all TransactionManagerLookup impls shipped with Infinispan for correctness
 *
 * @author Manik Surtani
 * @version 4.1
 */
@Test(testName = "tx.TransactionManagerLookupTest", groups = "unit")
public class TransactionManagerLookupTest extends AbstractInfinispanTest {
   
   Configuration configuration = new ConfigurationBuilder().build();

   public void testGenericTransactionManagerLookup() throws Exception {
      GenericTransactionManagerLookup lookup = new GenericTransactionManagerLookup();
      lookup.setConfiguration(configuration);
      doTest(lookup);
   }

   public void testDummyTransactionManagerLookup() throws Exception {
      doTest(new DummyTransactionManagerLookup());
   }

   public void testJBossStandaloneJTAManagerLookup() throws Exception {
      JBossStandaloneJTAManagerLookup lookup = new JBossStandaloneJTAManagerLookup();
      lookup.init(configuration);
      doTest(lookup);
   }
   
   protected void doTest(TransactionManagerLookup lookup) throws Exception {
      TransactionManager tm = lookup.getTransactionManager();
      tm.begin();
      tm.commit();

      tm.begin();
      tm.rollback();
   }

}

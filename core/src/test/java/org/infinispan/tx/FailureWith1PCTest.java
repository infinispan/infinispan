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

package org.infinispan.tx;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;

/**
 * Test for https://issues.jboss.org/browse/ISPN-1093.
 * @author Mircea Markus
 */
@Test (groups = "functional", testName = "tx.FailureWith1PCTest")
public class FailureWith1PCTest extends MultipleCacheManagersTest {

   boolean fail = true;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      c.fluent().hash().numOwners(3);
      createCluster(c, true, 3);
      waitForClusterToForm();
   }

   public void testInducedFailureOn1pc() throws Exception {

      cache(1).getAdvancedCache().addInterceptor(new CommandInterceptor() {

         @Override
         public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
            if (fail)
               throw new RuntimeException("Induced exception");
            else
               return invokeNextInterceptor(ctx, command);
         }
      }, 1);

      tm(0).begin();
      cache(0).put("k", "v");

      try {
         tm(0).commit();
         assert false : "Exception expected";
      } catch (Exception e) {
         //expected
         e.printStackTrace();
      }

      fail = false;

      assertExpectedState(0);
      assertExpectedState(1);
      assertExpectedState(2);

   }

   private void assertExpectedState(int index) {
      assertNull(cache(index).get("k"));
      assert !lockManager(index).isLocked("k");
      assert TestingUtil.getTransactionTable(cache(index)).getLocalTxCount() == 0;
      assert TestingUtil.getTransactionTable(cache(index)).getRemoteTxCount() == 0;
   }
}

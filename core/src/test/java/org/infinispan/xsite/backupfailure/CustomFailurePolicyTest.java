/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
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

package org.infinispan.xsite.backupfailure;

import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.xsite.CountingCustomFailurePolicy;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.backupfailure.CustomFailurePolicyTest")
public class CustomFailurePolicyTest extends NonTxBackupFailureTest{

   public CustomFailurePolicyTest() {
      lonBackupFailurePolicy = BackupFailurePolicy.CUSTOM;
      lonCustomFailurePolicyClass = CountingCustomFailurePolicy.class.getName();
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @Override
   public void testPutFailure() {
      assertFalse(CountingCustomFailurePolicy.PUT_INVOKED);
      super.testPutFailure();
      assertTrue(CountingCustomFailurePolicy.PUT_INVOKED);
   }

   @Override
   public void testRemoveFailure() {
      assertFalse(CountingCustomFailurePolicy.REMOVE_INVOKED);
      super.testRemoveFailure();
      assertTrue(CountingCustomFailurePolicy.REMOVE_INVOKED);
   }

   @Override
   public void testReplaceFailure() {
      assertFalse(CountingCustomFailurePolicy.REPLACE_INVOKED);
      super.testReplaceFailure();
      assertTrue(CountingCustomFailurePolicy.REPLACE_INVOKED);
   }

   @Override
   public void testClearFailure() {
      assertFalse(CountingCustomFailurePolicy.CLEAR_INVOKED);
      super.testClearFailure();
      assertTrue(CountingCustomFailurePolicy.CLEAR_INVOKED);
   }

   @Override
   public void testPutMapFailure() {
      assertFalse(CountingCustomFailurePolicy.PUT_ALL_INVOKED);
      super.testPutMapFailure();
      assertTrue(CountingCustomFailurePolicy.PUT_ALL_INVOKED);
   }
}

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
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite")
public class OptReplBackupFailure2Test extends NonTxBackupFailureTest {

   public OptReplBackupFailure2Test() {
      lonBackupFailurePolicy = BackupFailurePolicy.FAIL;
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }

   //todo - if I don't explicitly override the test methods then testNG won't execute them from superclass.
   //fix this once we move to JUnit

   @Override
   public void testPutFailure() {
      super.testPutFailure();
   }

   @Override
   public void testRemoveFailure() {
      super.testRemoveFailure();
   }

   @Override
   public void testReplaceFailure() {
      super.testReplaceFailure();
   }

   @Override
   public void testClearFailure() {
      super.testClearFailure();
   }

   @Override
   public void testPutMapFailure() {
      super.testPutMapFailure();
   }
}

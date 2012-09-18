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

package org.infinispan.xsite;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite")
public class PessReplCacheOperationsTest extends BaseCacheOperationsTest {

   protected ConfigurationBuilder getNycActiveConfig() {
      return getPessimisticReplCache();
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getPessimisticReplCache();
   }

   private ConfigurationBuilder getPessimisticReplCache() {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      dcc.transaction().lockingMode(LockingMode.PESSIMISTIC);
      return dcc;
   }

   //todo - if I don't explicitly override the test methods then testNG won't execute them from superclass.
   //fix this once we move to JUnit

   @Override
   public void testRemove() {
      super.testRemove();
   }

   @Override
   public void testPutAndClear() {
      super.testPutAndClear();
   }

   @Override
   public void testReplace() {
      super.testReplace();
   }

   @Override
   public void testPutAll() {
      super.testPutAll();
   }
}

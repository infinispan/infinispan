/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.stats.topK;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusterTopKeyTest;
import org.testng.annotations.Test;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "stats.topK.ReplTopKeyTest")
public class ReplTopKeyTest extends BaseClusterTopKeyTest {

   public ReplTopKeyTest() {
      super(CacheMode.REPL_SYNC, 2);
   }

   @Override
   protected boolean isOwner(Cache<?, ?> cache, Object key) {
      return true;
   }

   @Override
   protected boolean isPrimaryOwner(Cache<?, ?> cache, Object key) {
      return cache.getAdvancedCache().getRpcManager().getTransport().getCoordinator().equals(addressOf(cache));
   }
}

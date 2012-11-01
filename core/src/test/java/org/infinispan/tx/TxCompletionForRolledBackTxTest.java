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

package org.infinispan.tx;

import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.mocks.ControlledCommandFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "tx.TxCompletionForRolledBackTxTest")
public class TxCompletionForRolledBackTxTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().hash().numOwners(1).transaction().lockingMode(LockingMode.PESSIMISTIC);
      amend(dcc);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   protected void amend(ConfigurationBuilder dcc) {}

   public void testTxCompletionNotSentForRollback() throws Throwable {
      ControlledCommandFactory cf = ControlledCommandFactory.registerControlledCommandFactory(cache(1), null);

      tm(0).begin();
      Object k = getKeyForCache(1);
      cache(0).put(k,"k");
      tm(0).rollback();

      assertNotLocked(k);
      assertNull(cache(0).get(k));

      assertEquals(cf.received(RollbackCommand.class), 1);
      assertEquals(cf.received(TxCompletionNotificationCommand.class), 0);
   }
}

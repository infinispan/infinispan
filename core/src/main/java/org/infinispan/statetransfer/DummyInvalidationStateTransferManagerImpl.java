/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.statetransfer;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * Dummy StateTransferManager implementation for caches in invalidation mode.
 * 
 * It relies on the base class to register the cache with {@link org.infinispan.cacheviews.CacheViewsManager},
 * but it doesn't do anything else.
 * 
 * @author Dan Berindei
 * @since 5.1
 * @deprecated This is just a temporary hack, do not rely on it to exist in future versions
 */
@Deprecated
public class DummyInvalidationStateTransferManagerImpl extends BaseStateTransferManagerImpl {
   @Override
   protected ConsistentHash createConsistentHash(List<Address> members) {
      return null;
   }

   @Override
   public CacheStore getCacheStoreForStateTransfer() {
      return null;
   }

   @Override
   protected BaseStateTransferTask createStateTransferTask(final int viewId, final List<Address> members,
                                                           final boolean initialView) {
      return new BaseStateTransferTask(this, rpcManager, stateTransferLock, cacheNotifier, configuration, dataContainer,
            members, viewId, null, null, initialView) {
         @Override
         public void doPerformStateTransfer() throws Exception {
            // The state transfer lock is not really used in invalidation mode
            // but it's easier to block write commands here than override the base methods
            // to remove all the blocking and unblocking
            stateTransferLock.blockNewTransactions(viewId);
         }

         @Override
         public void commitStateTransfer() {
            // do nothing
         }
      };
   }

   @Override
   protected long getTimeout() {
      // although we don't have state transfer RPCs, we still have to wait for the join to complete
      return configuration.clustering().stateTransfer().timeout();
   }

   @Override
   public boolean isLocationInDoubt(Object key) {
      return false;
   }
}

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
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;

/**
 * The replicated mode implementation of {@link StateTransferManager}
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @since 5.1
 */
@MBean(objectName = "DistributedStateTransferManager", description = "Component that handles state transfer in distributed mode")
public class ReplicatedStateTransferManagerImpl extends BaseStateTransferManagerImpl {
   private static final Log log = LogFactory.getLog(ReplicatedStateTransferManagerImpl.class);

   /**
    * Default constructor
    */
   public ReplicatedStateTransferManagerImpl() {
      super();
   }

   protected ReplicatedStateTransferTask createStateTransferTask(int viewId, List<Address> members, boolean initialView) {
      return new ReplicatedStateTransferTask(rpcManager, configuration, dataContainer,
            this, stateTransferLock, cacheNotifier, viewId, members, chOld, chNew, initialView);
   }

   protected ConsistentHash createConsistentHash(List<Address> members) {
      // The user will not be able to configure the consistent hash in replicated mode
      // We are always going to use the default consistent hash function.
      return ConsistentHashHelper.createConsistentHash(configuration, members);
   }

   public CacheStore getCacheStoreForStateTransfer() {
      if (cacheLoaderManager == null || !cacheLoaderManager.isEnabled() || cacheLoaderManager.isShared()
            || !cacheLoaderManager.isFetchPersistentState())
         return null;
      return cacheLoaderManager.getCacheStore();
   }

   public boolean isLocationInDoubt(Object key) {
      return !isJoinComplete();
   }
}


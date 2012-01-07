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

import org.infinispan.CacheException;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.SKIP_LOCKING;

/**
 * The distributed mode implementation of {@link StateTransferManager}
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @author Bela Ban
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @since 4.0
 */
@MBean(objectName = "DistributedStateTransferManager", description = "Component that handles state transfer in distributed mode")
public class DistributedStateTransferManagerImpl extends BaseStateTransferManagerImpl {
   private static final Log log = LogFactory.getLog(DistributedStateTransferManagerImpl.class);

   private DistributionManager dm;

   /**
    * Default constructor
    */
   public DistributedStateTransferManagerImpl() {
      super();
   }

   @Inject
   public void init(DistributionManager dm) {
      this.dm = dm;
   }


   protected BaseStateTransferTask createStateTransferTask(int viewId, List<Address> members, boolean initialView) {
      return new DistributedStateTransferTask(rpcManager, configuration, dataContainer,
            this, dm, stateTransferLock, cacheNotifier, viewId, members, chOld, chNew, initialView);
   }

   protected ConsistentHash createConsistentHash(List<Address> members) {
      return ConsistentHashHelper.createConsistentHash(configuration, members);
   }

   public void invalidateKeys(List<Object> keysToRemove) {
      try {
         if (keysToRemove.size() > 0) {
            InvalidateCommand invalidateCmd = cf.buildInvalidateFromL1Command(true, keysToRemove);
            InvocationContext ctx = icc.createNonTxInvocationContext();
            ctx.setFlags(CACHE_MODE_LOCAL, SKIP_LOCKING);
            interceptorChain.invoke(ctx, invalidateCmd);

            log.debugf("Invalidated %d keys, data container now has %d keys", keysToRemove.size(), dataContainer.size());
            log.tracef("Invalidated keys: %s", keysToRemove);
         }
      } catch (CacheException e) {
         log.failedToInvalidateKeys(e);
      }
   }

   public CacheStore getCacheStoreForStateTransfer() {
      if (cacheLoaderManager == null || !cacheLoaderManager.isEnabled() || cacheLoaderManager.isShared())
         return null;
      return cacheLoaderManager.getCacheStore();
   }

   public boolean isLocationInDoubt(Object key) {
      return isStateTransferInProgress() && !chOld.locate(key, configuration.getNumOwners()).contains(getAddress())
            && chNew.locate(key, configuration.getNumOwners()).contains(getAddress());
   }
}


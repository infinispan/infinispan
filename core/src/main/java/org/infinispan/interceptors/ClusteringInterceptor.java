/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.concurrent.locks.LockManager;

import static org.infinispan.util.Util.toStr;

/**
 * Base class for replication and distribution interceptors.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public abstract class ClusteringInterceptor extends BaseRpcInterceptor {

   protected CommandsFactory cf;
   protected EntryFactory entryFactory;
   protected LockManager lockManager;
   protected DataContainer dataContainer;
   protected StateTransferManager stateTransferManager;
   protected boolean needReliableReturnValues;

   @Inject
   public void injectDependencies(CommandsFactory cf, EntryFactory entryFactory,
                                  LockManager lockManager, DataContainer dataContainer,
                                  StateTransferManager stateTransferManager) {
      this.cf = cf;
      this.entryFactory = entryFactory;
      this.lockManager = lockManager;
      this.dataContainer = dataContainer;
      this.stateTransferManager = stateTransferManager;
   }

   @Start
   public void configure() {
      needReliableReturnValues = !cacheConfiguration.unsafe().unreliableReturnValues();
   }

   protected boolean isNeedReliableReturnValues(FlagAffectedCommand command) {
      return !command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
            && !command.hasFlag(Flag.IGNORE_RETURN_VALUES) && needReliableReturnValues;
   }

   protected boolean needsRemoteGet(InvocationContext ctx, AbstractDataCommand command) {
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)
            || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
            || command.hasFlag(Flag.IGNORE_RETURN_VALUES)) {
         return false;
      }
      boolean shouldFetchFromRemote = false;
      CacheEntry entry = ctx.lookupEntry(command.getKey());
      if (entry == null || entry.isNull()) {
         Object key = command.getKey();
         ConsistentHash ch = stateTransferManager.getCacheTopology().getReadConsistentHash();
         shouldFetchFromRemote = ctx.isOriginLocal() && !ch.isKeyLocalToNode(rpcManager.getAddress(), key) && !dataContainer.containsKey(key);
         if (!shouldFetchFromRemote && getLog().isTraceEnabled()) {
            getLog().tracef("Not doing a remote get for key %s since entry is mapped to current node (%s) or is in L1. Owners are %s", toStr(key), rpcManager.getAddress(), ch.locateOwners(key));
         }
      }
      return shouldFetchFromRemote;
   }

   /**
    * For conditional operations (replace, remove, put if absent) Used only for optimistic transactional caches, to solve the following situation:
    * <pre>
    * - node A (owner, tx originator) does a successful replace
    * - the actual value changes
    * - tx commits. The value is applied on A (the check was performed at operation time) but is not applied on
    *   B (check is performed at commit time).
    * In such situations (optimistic caches) the remote conditional command should not re-check the old value.
    * </pre>
    */
   protected boolean ignorePreviousValueOnBackup(WriteCommand command, InvocationContext ctx) {
      return ctx.isOriginLocal() && command.isSuccessful();
   }

   /**
    * Retrieves a cache entry from a remote source.  Would typically involve an RPC call using a {@link org.infinispan.commands.remote.ClusteredGetCommand}
    * and some form of quorum of responses if the responses returned are inconsistent - often the case if there is a
    * rehash in progress, involving nodes that the key maps to.
    *
    * @param key key to look up
    * @return an internal cache entry, or null if it cannot be located
    */
   protected abstract InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx, boolean acquireRemoteLock, FlagAffectedCommand command) throws Exception;
}

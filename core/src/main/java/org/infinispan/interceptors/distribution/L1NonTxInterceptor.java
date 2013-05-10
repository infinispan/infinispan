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

package org.infinispan.interceptors.distribution;

import org.infinispan.Metadata;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.L1Manager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Interceptor that handles L1 logic for non-transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class L1NonTxInterceptor extends BaseRpcInterceptor {

   private static Log log = LogFactory.getLog(L1NonTxInterceptor.class);

   private L1Manager l1Manager;
   private ClusteringDependentLogic cdl;
   private CommandsFactory cf;
   private LockManager lockManager;
   private EntryFactory entryFactory;

   @Inject
   public void init(L1Manager l1Manager, ClusteringDependentLogic cdl, LockManager lockManager,
                    EntryFactory entryFactory, CommandsFactory cf) {
      this.l1Manager = l1Manager;
      this.cdl = cdl;
      this.lockManager = lockManager;
      this.entryFactory = entryFactory;
      this.cf = cf;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      InternalCacheEntry ice = command.getRemotelyFetchedValue();
      if (ctx.isOriginLocal() && ice != null) {
         log.tracef("Caching remotely retrieved entry for key %s in L1", command.getKey());
         // This should be fail-safe
         try {
            long l1Lifespan = cacheConfiguration.clustering().l1().lifespan();
            long lifespan = ice.getLifespan() < 0 ? l1Lifespan : Math.min(ice.getLifespan(), l1Lifespan);
            // Make a copy of the metadata stored internally, adjust
            // lifespan/maxIdle settings and send them a modification
            Metadata newMetadata = ice.getMetadata().builder()
                  .lifespan(lifespan, TimeUnit.MILLISECONDS)
                  .maxIdle(-1, TimeUnit.MILLISECONDS).build();
            PutKeyValueCommand put = cf.buildPutKeyValueCommand(ice.getKey(), ice.getValue(),
                  newMetadata, Collections.singleton(Flag.CACHE_MODE_LOCAL));
            lockAndWrap(ctx, command.getKey(), ice, command);
            invokeNextInterceptor(ctx, put);
         } catch (Exception e) {
            // Couldn't store in L1 for some reason.  But don't fail the transaction!
            log.infof("Unable to store entry %s in L1 cache", command.getKey());
            log.debug("Inability to store in L1 caused by", e);
         }

      } else if (!ctx.isOriginLocal() && returnValue != null) {
         l1Manager.addRequestor(command.getKey(), ctx.getOrigin());
      }
      return returnValue;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, true);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, true);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Future<Object> invalidationFuture = null;
      Set<Object> keys = command.getMap().keySet();
      Set<Object> toInvalidate = new HashSet<Object>(keys.size());
      for (Object k : keys) {
         if (cdl.localNodeIsOwner(k)) {
            toInvalidate.add(k);
         }
      }
      if (!toInvalidate.isEmpty()) {
         invalidationFuture = l1Manager.flushCache(toInvalidate, ctx.getOrigin(), true);
      }

      Object result = invokeNextInterceptor(ctx, command);
      processInvalidationResult(ctx, command, invalidationFuture);
      //we also need to remove from L1 the keys that are not ours
      for (Object o : command.getAffectedKeys()) {
         if (!cdl.localNodeIsOwner(o)) {
            removeFromL1(ctx, o);
         }
      }
      return result;
   }

   private Object handleDataWriteCommand(InvocationContext ctx, DataWriteCommand command, boolean assumeOriginKeptEntryInL1) throws Throwable {
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         log.tracef("local mode forced, suppressing L1 calls.");
         return invokeNextInterceptor(ctx, command);
      }
      Future<Object> l1InvalidationFuture = invalidateL1(ctx, command, assumeOriginKeptEntryInL1);
      Object returnValue = invokeNextInterceptor(ctx, command);
      processInvalidationResult(ctx, command, l1InvalidationFuture);
      removeFromLocalL1(ctx, command);
      return returnValue;
   }

   private void removeFromLocalL1(InvocationContext ctx, DataWriteCommand command) throws InterruptedException {
      if (ctx.isOriginLocal() && !cdl.localNodeIsOwner(command.getKey())) {
         removeFromL1(ctx, command.getKey());
      } else {
         log.trace("Allowing entry to commit as local node is owner");
      }
   }

   private void removeFromL1(InvocationContext ctx, Object key) throws InterruptedException {
      log.tracef("Removing entry from L1 for key %s", key);
      ctx.removeLookedUpEntry(key);
      entryFactory.wrapEntryForRemove(ctx, key);
   }

   private void processInvalidationResult(InvocationContext ctx, FlagAffectedCommand command, Future<Object> l1InvalidationFuture) throws InterruptedException, ExecutionException {
      if (l1InvalidationFuture != null) {
         if (isSynchronous(command)) {
            l1InvalidationFuture.get();
         }
      }
   }

   private Future<Object> invalidateL1(InvocationContext ctx, DataWriteCommand command, boolean assumeOriginKeptEntryInL1) {
      Future<Object> l1InvalidationFuture = null;
      if (cdl.localNodeIsOwner(command.getKey())) {
         l1InvalidationFuture = l1Manager.flushCache(Collections.singletonList(command.getKey()), ctx.getOrigin(), assumeOriginKeptEntryInL1);
      } else  {
         log.tracef("Not invalidating key '%' as local node(%s) is not owner", command.getKey(), rpcManager.getAddress());
      }
      return l1InvalidationFuture;
   }

   private void lockAndWrap(InvocationContext ctx, Object key, InternalCacheEntry ice, FlagAffectedCommand command) throws InterruptedException {
      boolean skipLocking = hasSkipLocking(command);
      long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
      lockManager.acquireLock(ctx, key, lockTimeout, skipLocking);
      entryFactory.wrapEntryForPut(ctx, key, ice, false, command);
   }
}

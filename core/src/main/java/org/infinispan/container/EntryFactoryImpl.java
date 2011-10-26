/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.container;

import org.infinispan.config.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullMarkerEntry;
import org.infinispan.container.entries.NullMarkerEntryForRemoval;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.container.entries.RepeatableReadEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link EntryFactory} implementation to be used for optimistic locking scheme.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class EntryFactoryImpl implements EntryFactory {

   private static final Log log = LogFactory.getLog(EntryFactoryImpl.class);
   private final boolean trace = log.isTraceEnabled();
   
   private boolean useRepeatableRead;
   private DataContainer container;
   private boolean writeSkewCheck;
   private Configuration configuration;
   private CacheNotifier notifier;

   @Inject
   public void injectDependencies(DataContainer dataContainer, Configuration configuration, CacheNotifier notifier) {
      this.container = dataContainer;
      this.configuration = configuration;
      this.notifier = notifier;
   }

   @Start
   public void init() {
      useRepeatableRead = configuration.getIsolationLevel() == IsolationLevel.REPEATABLE_READ;
      writeSkewCheck = configuration.isWriteSkewCheck();
   }

   @Override
   public final CacheEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      if (cacheEntry == null) {
         cacheEntry = getFromContainer(key);

         // do not bother wrapping though if this is not in a tx.  repeatable read etc are all meaningless unless there is a tx.
         if (useRepeatableRead) {
            MVCCEntry mvccEntry = cacheEntry == null ?
                  createWrappedEntry(key, null, false, false, -1) :
                  createWrappedEntry(key, cacheEntry.getValue(), false, false, cacheEntry.getLifespan());
            if (mvccEntry != null) ctx.putLookedUpEntry(key, mvccEntry);
            return mvccEntry;
         } else if (cacheEntry != null) { // if not in transaction and repeatable read, or simply read committed (regardless of whether in TX or not), do not wrap
            ctx.putLookedUpEntry(key, cacheEntry);
         }
         return cacheEntry;
      }
      return cacheEntry;
   }

   @Override
   public final  MVCCEntry wrapEntryForClear(InvocationContext ctx, Object key) throws InterruptedException {
      return wrapEntry(ctx, key);
   }

   @Override
   public final  MVCCEntry wrapEntryForReplace(InvocationContext ctx, Object key) throws InterruptedException {
      MVCCEntry mvccEntry = wrapEntry(ctx, key);
      if (mvccEntry == null) {
         // make sure we record this! Null value since this is a forced lock on the key
         ctx.putLookedUpEntry(key, null);
      }
      return mvccEntry;
   }

   @Override
   public final  MVCCEntry wrapEntryForRemove(InvocationContext ctx, Object key) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      MVCCEntry mvccEntry = null;
      if (cacheEntry != null) {
         if (cacheEntry instanceof MVCCEntry && !(cacheEntry instanceof NullMarkerEntry)) {
            mvccEntry = (MVCCEntry) cacheEntry;
         } else {
            mvccEntry = wrapMvccEntryForRemove(ctx, key, cacheEntry);
         }
      } else {
         InternalCacheEntry ice = getFromContainer(key);
         if (ice != null) {
            mvccEntry = wrapInternalCacheEntryForPut(ctx, key, ice);
         }
      }
      if (mvccEntry == null) {
         // make sure we record this! Null value since this is a forced lock on the key
         ctx.putLookedUpEntry(key, null);
      } else {
         mvccEntry.copyForUpdate(container, writeSkewCheck);
      }
      return mvccEntry;
   }

   @Override
   public final MVCCEntry wrapEntryForPut(InvocationContext ctx, Object key, InternalCacheEntry icEntry, boolean undeleteIfNeeded) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      MVCCEntry mvccEntry;
      if (cacheEntry != null && cacheEntry.isNull()) cacheEntry = null;
      if (cacheEntry != null) {
         mvccEntry = wrapMvccEntryForPut(ctx, key, cacheEntry);
         mvccEntry.undelete(undeleteIfNeeded);
      } else {
         InternalCacheEntry ice = (icEntry == null ? getFromContainer(key) : icEntry);
         mvccEntry = ice != null ?
             wrapInternalCacheEntryForPut(ctx, key, ice) :
             newMvccEntryForPut(ctx, key);
      }
      mvccEntry.copyForUpdate(container, writeSkewCheck);
      return mvccEntry;
   }

   private CacheEntry getFromContext(InvocationContext ctx, Object key) {
      final CacheEntry cacheEntry = ctx.lookupEntry(key);
      if (trace) log.tracef("Exists in context? %s ", cacheEntry);
      return cacheEntry;
   }

   private InternalCacheEntry getFromContainer(Object key) {
      final InternalCacheEntry ice = container.get(key);
      if (trace) log.tracef("Retrieved from container %s", ice);
      return ice;
   }

   private MVCCEntry newMvccEntryForPut(InvocationContext ctx, Object key) {
      MVCCEntry mvccEntry;
      if (trace) log.trace("Creating new entry.");
      notifier.notifyCacheEntryCreated(key, true, ctx);
      mvccEntry = createWrappedEntry(key, null, true, false, -1);
      mvccEntry.setCreated(true);
      ctx.putLookedUpEntry(key, mvccEntry);
      notifier.notifyCacheEntryCreated(key, false, ctx);
      return mvccEntry;
   }

   private MVCCEntry wrapMvccEntryForPut(InvocationContext ctx, Object key, CacheEntry cacheEntry) {
      if (cacheEntry instanceof MVCCEntry) return (MVCCEntry) cacheEntry;
      return wrapInternalCacheEntryForPut(ctx, key, (InternalCacheEntry) cacheEntry);
   }

   private MVCCEntry wrapInternalCacheEntryForPut(InvocationContext ctx, Object key, InternalCacheEntry cacheEntry) {
      MVCCEntry mvccEntry = createWrappedEntry(key, cacheEntry.getValue(), false, false, cacheEntry.getLifespan());
      ctx.putLookedUpEntry(key, mvccEntry);
      return mvccEntry;
   }

   private MVCCEntry wrapMvccEntryForRemove(InvocationContext ctx, Object key, CacheEntry cacheEntry) {
      MVCCEntry mvccEntry = createWrappedEntry(key, cacheEntry.getValue(), false, true, cacheEntry.getLifespan());
      ctx.putLookedUpEntry(key, mvccEntry);
      return mvccEntry;
   }

   private MVCCEntry wrapEntry(InvocationContext ctx, Object key) {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      MVCCEntry mvccEntry = null;
      if (cacheEntry != null) {
         mvccEntry = wrapMvccEntryForPut(ctx, key, cacheEntry);
      } else {
         InternalCacheEntry ice = getFromContainer(key);
         if (ice != null) {
            mvccEntry = wrapInternalCacheEntryForPut(ctx, ice.getKey(), ice);
         }
      }
      if (mvccEntry != null)
         mvccEntry.copyForUpdate(container, writeSkewCheck);
      return mvccEntry;
   }

   private  MVCCEntry createWrappedEntry(Object key, Object value, boolean isForInsert, boolean forRemoval, long lifespan) {
      if (value == null && !isForInsert) return useRepeatableRead ?
            forRemoval ? new NullMarkerEntryForRemoval(key) : NullMarkerEntry.getInstance()
            : null;

      return useRepeatableRead ? new RepeatableReadEntry(key, value, lifespan) : new ReadCommittedEntry(key, value, lifespan);
   }
}

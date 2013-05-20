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

import org.infinispan.Metadata;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.DeltaAwareCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullMarkerEntry;
import org.infinispan.container.entries.NullMarkerEntryForRemoval;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.container.entries.RepeatableReadEntry;
import org.infinispan.container.entries.StateChangingEntry;
import org.infinispan.context.Flag;
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
   
   protected boolean useRepeatableRead;
   private DataContainer container;
   protected boolean localModeWriteSkewCheck;
   private Configuration configuration;
   private CacheNotifier notifier;

   @Inject
   public void injectDependencies(DataContainer dataContainer, Configuration configuration, CacheNotifier notifier) {
      this.container = dataContainer;
      this.configuration = configuration;
      this.notifier = notifier;
   }

   @Start (priority = 8)
   public void init() {
      useRepeatableRead = configuration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
      localModeWriteSkewCheck = configuration.locking().writeSkewCheck();
   }

   @Override
   public final CacheEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      if (cacheEntry == null) {
         cacheEntry = getFromContainer(key);

         // do not bother wrapping though if this is not in a tx.  repeatable read etc are all meaningless unless there is a tx.
         if (useRepeatableRead) {
            MVCCEntry mvccEntry;
            if (cacheEntry == null) {
               mvccEntry = createWrappedEntry(key, null, null, false, false);
            } else {
               mvccEntry = createWrappedEntry(key, cacheEntry, null, false, false);
               // If the original entry has changeable state, copy state flags to the new MVCC entry.
               if (cacheEntry instanceof StateChangingEntry && mvccEntry != null)
                  mvccEntry.copyStateFlagsFrom((StateChangingEntry) cacheEntry);
            }

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
      return wrapEntry(ctx, key, null);
   }

   @Override
   public final  MVCCEntry wrapEntryForReplace(InvocationContext ctx, ReplaceCommand cmd) throws InterruptedException {
      Object key = cmd.getKey();
      MVCCEntry mvccEntry = wrapEntry(ctx, key, cmd.getMetadata());
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
            mvccEntry = wrapInternalCacheEntryForPut(ctx, key, ice, null);
            mvccEntry.setRemoved(true);
         }
      }
      if (mvccEntry == null) {
         // make sure we record this! Null value since this is a forced lock on the key
         ctx.putLookedUpEntry(key, null);
      } else {
         mvccEntry.copyForUpdate(container, localModeWriteSkewCheck);
      }
      return mvccEntry;
   }

   @Override
   public final MVCCEntry wrapEntryForPut(InvocationContext ctx, Object key, InternalCacheEntry icEntry,
         boolean undeleteIfNeeded, FlagAffectedCommand cmd) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      MVCCEntry mvccEntry;
      if (cacheEntry != null && cacheEntry.isNull()) cacheEntry = null;
      Metadata providedMetadata = cmd.getMetadata();
      if (cacheEntry != null) {
         mvccEntry = wrapMvccEntryForPut(ctx, key, cacheEntry, providedMetadata);
         mvccEntry.undelete(undeleteIfNeeded);
      } else {
         InternalCacheEntry ice = (icEntry == null ? getFromContainer(key) : icEntry);
         // A putForExternalRead is putIfAbsent, so if key present, do nothing
         if (ice != null && cmd.hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
            // make sure we record this! Null value since this is a forced lock on the key
            ctx.putLookedUpEntry(key, null);
            return null;
         }

         mvccEntry = ice != null ?
             wrapInternalCacheEntryForPut(ctx, key, ice, providedMetadata) :
             newMvccEntryForPut(ctx, key, cmd, providedMetadata);
      }
      mvccEntry.copyForUpdate(container, localModeWriteSkewCheck);
      return mvccEntry;
   }
   
   @Override
   public CacheEntry wrapEntryForDelta(InvocationContext ctx, Object deltaKey, Delta delta ) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, deltaKey);
      DeltaAwareCacheEntry deltaAwareEntry = null;
      if (cacheEntry != null) {        
         deltaAwareEntry = wrapEntryForDelta(ctx, deltaKey, cacheEntry);
      } else {                     
         InternalCacheEntry ice = getFromContainer(deltaKey);
         if (ice != null){
            deltaAwareEntry = newDeltaAwareCacheEntry(ctx, deltaKey, (DeltaAware)ice.getValue());
         }
      }
      if (deltaAwareEntry != null)
         deltaAwareEntry.appendDelta(delta);
      return deltaAwareEntry;
   }
   
   private DeltaAwareCacheEntry wrapEntryForDelta(InvocationContext ctx, Object key, CacheEntry cacheEntry) {
      if (cacheEntry instanceof DeltaAwareCacheEntry) return (DeltaAwareCacheEntry) cacheEntry;
      return wrapInternalCacheEntryForDelta(ctx, key, cacheEntry);
   }
   
   private DeltaAwareCacheEntry wrapInternalCacheEntryForDelta(InvocationContext ctx, Object key, CacheEntry cacheEntry) {
      DeltaAwareCacheEntry e;
      if(cacheEntry instanceof MVCCEntry){
         e = createWrappedDeltaEntry(key, (DeltaAware) cacheEntry.getValue(), cacheEntry);
      }
      else if (cacheEntry instanceof InternalCacheEntry) {
         cacheEntry = wrapInternalCacheEntryForPut(ctx, key, (InternalCacheEntry) cacheEntry, null);
         e = createWrappedDeltaEntry(key, (DeltaAware) cacheEntry.getValue(), cacheEntry);
      }
      else {
         e = createWrappedDeltaEntry(key, (DeltaAware) cacheEntry.getValue(), null);
      }
      ctx.putLookedUpEntry(key, e);
      return e;

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

   private MVCCEntry newMvccEntryForPut(
         InvocationContext ctx, Object key, FlagAffectedCommand cmd, Metadata providedMetadata) {
      MVCCEntry mvccEntry;
      if (trace) log.trace("Creating new entry.");
      notifier.notifyCacheEntryCreated(key, null, true, ctx, cmd);
      mvccEntry = createWrappedEntry(key, null, providedMetadata, true, false);
      mvccEntry.setCreated(true);
      ctx.putLookedUpEntry(key, mvccEntry);
      return mvccEntry;
   }

   private MVCCEntry wrapMvccEntryForPut(InvocationContext ctx, Object key, CacheEntry cacheEntry, Metadata providedMetadata) {
      if (cacheEntry instanceof MVCCEntry) return (MVCCEntry) cacheEntry;
      return wrapInternalCacheEntryForPut(ctx, key, (InternalCacheEntry) cacheEntry, providedMetadata);
   }

   private MVCCEntry wrapInternalCacheEntryForPut(InvocationContext ctx, Object key, InternalCacheEntry cacheEntry, Metadata providedMetadata) {
      MVCCEntry mvccEntry = createWrappedEntry(key, cacheEntry, providedMetadata, false, false);
      ctx.putLookedUpEntry(key, mvccEntry);
      return mvccEntry;
   }

   private MVCCEntry wrapMvccEntryForRemove(InvocationContext ctx, Object key, CacheEntry cacheEntry) {
      MVCCEntry mvccEntry = createWrappedEntry(key, cacheEntry, null, false, true);
      // If the original entry has changeable state, copy state flags to the new MVCC entry.
      if (cacheEntry instanceof StateChangingEntry)
         mvccEntry.copyStateFlagsFrom((StateChangingEntry) cacheEntry);

      ctx.putLookedUpEntry(key, mvccEntry);
      return mvccEntry;
   }

   private MVCCEntry wrapEntry(InvocationContext ctx, Object key, Metadata providedMetadata) {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      MVCCEntry mvccEntry = null;
      if (cacheEntry != null) {
         mvccEntry = wrapMvccEntryForPut(ctx, key, cacheEntry, providedMetadata);
      } else {
         InternalCacheEntry ice = getFromContainer(key);
         if (ice != null) {
            mvccEntry = wrapInternalCacheEntryForPut(ctx, ice.getKey(), ice, providedMetadata);
         }
      }
      if (mvccEntry != null)
         mvccEntry.copyForUpdate(container, localModeWriteSkewCheck);
      return mvccEntry;
   }

   protected MVCCEntry createWrappedEntry(Object key, CacheEntry cacheEntry,
         Metadata providedMetadata, boolean isForInsert, boolean forRemoval) {
      Object value = cacheEntry != null ? cacheEntry.getValue() : null;
      Metadata metadata = providedMetadata != null
            ? providedMetadata
            : cacheEntry != null ? cacheEntry.getMetadata() : null;

      if (value == null && !isForInsert) return useRepeatableRead ?
            forRemoval ? new NullMarkerEntryForRemoval(key, metadata) : NullMarkerEntry.getInstance()
            : null;

      return useRepeatableRead
            ? new RepeatableReadEntry(key, value, metadata)
            : new ReadCommittedEntry(key, value, metadata);
   }
   
   private DeltaAwareCacheEntry newDeltaAwareCacheEntry(InvocationContext ctx, Object key, DeltaAware deltaAware){
      DeltaAwareCacheEntry deltaEntry = createWrappedDeltaEntry(key, deltaAware, null);
      ctx.putLookedUpEntry(key, deltaEntry);
      return deltaEntry;
   }
   
   private DeltaAwareCacheEntry createWrappedDeltaEntry(Object key, DeltaAware deltaAware, CacheEntry entry) {
      return new DeltaAwareCacheEntry(key,deltaAware, entry);
   }

}

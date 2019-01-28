package org.infinispan.interceptors.impl;


import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.functional.impl.EntryViews.snapshot;

import java.lang.invoke.MethodHandles;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.Mutation;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.TxReadOnlyKeyCommand;
import org.infinispan.commands.functional.TxReadOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.SpliteratorMapper;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.group.impl.GroupManager;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.Param;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.StatsEnvelope;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.Metadatas;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.stream.impl.local.SegmentedEntryStreamSupplier;
import org.infinispan.stream.impl.local.SegmentedKeyStreamSupplier;
import org.infinispan.util.DataContainerRemoveIterator;
import org.infinispan.util.EntryWrapper;
import org.infinispan.util.UserRaisedFunctionalException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.rxjava.FlowableFromIntSetFunction;
import org.infinispan.reactive.RxJavaInterop;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.internal.functions.Functions;

/**
 * Always at the end of the chain, directly in front of the cache. Simply calls into the cache using reflection. If the
 * call resulted in a modification, add the Modification to the end of the modification list keyed by the current
 * transaction.
 *
 * @author Bela Ban
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 9.0
 */
public class CallInterceptor extends BaseAsyncInterceptor implements Visitor {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   // The amount in milliseconds of a buffer we allow the system clock to be off, but still allow expiration removal
   private static final int CLOCK_BUFFER = 100;

   @Inject ComponentRef<Cache> cacheRef;
   @Inject CacheNotifier cacheNotifier;
   @Inject TimeService timeService;
   @Inject VersionGenerator versionGenerator;
   @Inject InternalDataContainer<?, ?> dataContainer;
   @Inject DistributionManager distributionManager;
   @Inject InternalEntryFactory internalEntryFactory;
   @Inject KeyPartitioner keyPartitioner;
   @Inject GroupManager groupManager;

   // Internally we always deal with an unwrapped cache, so don't unwrap per invocation
   Cache unwrappedCache;

   private IncrementableEntryVersion nonExistentVersion;

   @Start
   public void start() {
      nonExistentVersion = versionGenerator.nonExistingVersion();
      unwrappedCache = AbstractDelegatingCache.unwrapCache(cacheRef.wired());
   }

   @Override
   public Object visitCommand(InvocationContext ctx, VisitableCommand command)
         throws Throwable {
      if (trace)
         log.tracef("Invoking: %s", command.getClass().getSimpleName());
      return command.acceptVisitor(ctx, this);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      // It's not worth looking up the entry if we're never going to apply the change.
      ValueMatcher valueMatcher = command.getValueMatcher();
      if (valueMatcher == ValueMatcher.MATCH_NEVER) {
         command.fail();
         return null;
      }
      //noinspection unchecked
      Object key = command.getKey();
      MVCCEntry<Object, Object> e = (MVCCEntry) ctx.lookupEntry(key);

      if (e == null) {
         throw new IllegalStateException("Not wrapped");
      }

      Object newValue = command.getValue();
      Metadata metadata = command.getMetadata();
      if (metadata instanceof InternalMetadataImpl) {
         InternalMetadataImpl internalMetadata = (InternalMetadataImpl) metadata;
         metadata = internalMetadata.actual();
         e.setCreated(internalMetadata.created());
         e.setLastUsed(internalMetadata.lastUsed());
      }
      Object prevValue = e.getValue();
      if (!valueMatcher.matches(prevValue, null, newValue)) {
         command.fail();
         return prevValue;
      }

      return performPut(e, ctx, valueMatcher, key, newValue, metadata, command,
                        command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER | FlagBitSets.PUT_FOR_X_SITE_STATE_TRANSFER));
   }

   private Object performPut(MVCCEntry<Object, Object> e, InvocationContext ctx, ValueMatcher valueMatcher,
         Object key, Object value, Metadata metadata, FlagAffectedCommand command, boolean skipNotification) {
      Object entryValue = e.getValue();
      Object o;

      // Non tx and tx both have this set if it was state transfer
      if (!skipNotification) {
         if (e.isCreated()) {
            cacheNotifier.notifyCacheEntryCreated(key, value, metadata, true, ctx, command);
         } else {
            cacheNotifier.notifyCacheEntryModified(key, value, metadata, entryValue, e.getMetadata(), true, ctx, command);
         }
      }

      o = e.setValue(value);
      Metadatas.updateMetadata(e, metadata);
      if (e.isRemoved()) {
         e.setCreated(true);
         e.setExpired(false);
         e.setRemoved(false);
         o = null;
      }
      e.setChanged(true);
      // Return the expected value when retrying a putIfAbsent command (i.e. null)
      return valueMatcher != ValueMatcher.MATCH_EXPECTED_OR_NEW ? o : null;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return visitRemoveCommand(ctx, command, true);
   }

   private Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command, boolean notifyRemove) throws Throwable {
      // It's not worth looking up the entry if we're never going to apply the change.
      ValueMatcher valueMatcher = command.getValueMatcher();
      if (valueMatcher == ValueMatcher.MATCH_NEVER) {
         command.fail();
         return null;
      }
      Object key = command.getKey();
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      Object prevValue = e.getValue();
      Object optionalValue = command.getValue();
      if (prevValue == null) {
         command.nonExistant();
         if (valueMatcher.matches(null, optionalValue, null)) {
            e.setChanged(true);
            e.setRemoved(true);
            e.setCreated(false);
            if (command instanceof EvictCommand) {
               e.setEvicted(true);
            }
            e.setValue(null);
            return command.isConditional() ? true : null;
         } else {
            log.trace("Nothing to remove since the entry doesn't exist in the context or it is null");
            command.fail();
            return false;
         }
      }

      if (!valueMatcher.matches(prevValue, optionalValue, null)) {
         command.fail();
         return false;
      }

      // Refactor this eventually
      if (command instanceof EvictCommand) {
         e.setEvicted(true);
      }

      return performRemove(e, ctx, valueMatcher, key, prevValue, optionalValue, notifyRemove, command);
   }

   protected Object performRemove(MVCCEntry e, InvocationContext ctx, ValueMatcher valueMatcher, Object key,
         Object prevValue, Object optionalValue, boolean notifyRemove, RemoveCommand command) {

      if (notifyRemove) {
         cacheNotifier.notifyCacheEntryRemoved(key, prevValue, e.getMetadata(), true, ctx, command);
      }

      e.setRemoved(true);
      e.setChanged(true);
      e.setValue(null);
      Metadata commandMetadata = command.getMetadata();
      if (commandMetadata != null) {
         e.setMetadata(commandMetadata);
      }

      if (valueMatcher != ValueMatcher.MATCH_EXPECTED_OR_NEW) {
         return command.isConditional() ? true : prevValue;
      } else {
         // Return the expected value when retrying
         return command.isConditional() ? true : optionalValue;
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      ValueMatcher valueMatcher = command.getValueMatcher();
      // It's not worth looking up the entry if we're never going to apply the change.
      if (valueMatcher == ValueMatcher.MATCH_NEVER) {
         command.fail();
         // TODO: this seems like a bug.. ?
         return null;
      }
      Object key = command.getKey();
      //noinspection unchecked
      MVCCEntry<Object, Object> e = (MVCCEntry) ctx.lookupEntry(key);
      // We need the null check as in non-tx caches we don't always wrap the entry on the origin
      Object prevValue = e.getValue();
      Object newValue = command.getNewValue();
      Object expectedValue = command.getOldValue();
      if (valueMatcher.matches(prevValue, expectedValue, newValue)) {
         e.setChanged(true);
         e.setValue(newValue);
         Metadata newMetadata = command.getMetadata();
         Metadata prevMetadata = e.getMetadata();

         cacheNotifier.notifyCacheEntryModified(key, newValue, newMetadata,
               expectedValue == null ? prevValue : expectedValue, prevMetadata, true, ctx, command);

         Metadatas.updateMetadata(e, newMetadata);

         return expectedValue == null ? prevValue : true;
      }

      command.fail();
      return expectedValue == null ? prevValue : false;
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      Object key = command.getKey();
      MVCCEntry<Object, Object> e = (MVCCEntry) ctx.lookupEntry(key);

      if (e == null) {
         throw new IllegalStateException("Not wrapped");
      }

      Object oldValue = e.getValue();
      Object newValue;


      if (command.isComputeIfPresent() && oldValue == null) {
         command.fail();
         return null;
      }

      try {
         newValue = command.getRemappingBiFunction().apply(key, oldValue);
      } catch (RuntimeException ex) {
         throw new UserRaisedFunctionalException(ex);
      }

      if (oldValue == null && newValue == null) {
         return null;
      }

      Metadata metadata = command.getMetadata();
      if (oldValue != null) {
         // The key already has a value
         if (newValue != null) {
            //replace with the new value if there is a modification on the value
            cacheNotifier.notifyCacheEntryModified(key, newValue, metadata, oldValue, e.getMetadata(), true, ctx, command);
            e.setChanged(true);
            e.setValue(newValue);
            Metadatas.updateMetadata(e, metadata);
         } else {
            // remove when new value is null
            cacheNotifier.notifyCacheEntryRemoved(key, oldValue, e.getMetadata(), true, ctx, command);
            e.setRemoved(true);
            e.setChanged(true);
            e.setValue(null);
         }
      } else {
         // put if not present
         cacheNotifier.notifyCacheEntryCreated(key, newValue, metadata, true, ctx, command);
         e.setValue(newValue);
         e.setChanged(true);
         Metadatas.updateMetadata(e, metadata);
         if (e.isRemoved()) {
            e.setCreated(true);
            e.setExpired(false);
            e.setRemoved(false);
         }
      }
      return newValue;
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      Object key = command.getKey();
      MVCCEntry<Object, Object> e = (MVCCEntry) ctx.lookupEntry(key);

      if (e == null) {
         throw new IllegalStateException("Not wrapped");
      }

      Object value = e.getValue();

      if (value == null) {
         try {
            value = command.getMappingFunction().apply(key);
         } catch (RuntimeException ex) {
            throw new UserRaisedFunctionalException(ex);
         }

         if (value != null) {
            e.setValue(value);
            Metadata metadata = command.getMetadata();
            Metadatas.updateMetadata(e, metadata);
            // TODO: should this be below?
            if (e.isCreated()) {
               cacheNotifier.notifyCacheEntryCreated(key, value, metadata, true, ctx, command);
            }
            if (e.isRemoved()) {
               e.setCreated(true);
               e.setExpired(false);
               e.setRemoved(false);
            }
            e.setChanged(true);
         }
      } else {
         command.fail();
      }
      return value;
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      for (InternalCacheEntry e : dataContainer) {
         cacheNotifier.notifyCacheEntryRemoved(e.getKey(), e.getValue(), e.getMetadata(), true, ctx, command);
      }
      return null;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Map<Object, Object> inputMap = command.getMap();
      Metadata metadata = command.getMetadata();
      // Previous values are used by the query interceptor to locate the index for the old value
      Map<Object, Object> previousValues = command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) ? null :
            new HashMap<>(inputMap.size());
      for (Map.Entry<Object, Object> e : inputMap.entrySet()) {
         Object key = e.getKey();
         MVCCEntry<Object, Object> contextEntry = lookupMvccEntry(ctx, key);
         if (contextEntry != null) {
            Object newValue = e.getValue();
            Object previousValue = contextEntry.getValue();
            Metadata previousMetadata = contextEntry.getMetadata();

            // Even though putAll() returns void, QueryInterceptor reads the previous values
            // TODO The previous values are not correct if the entries exist only in a store
            // We have to add null values due to the handling in distribution interceptor, see ISPN-7975
            if (previousValues != null) {
               previousValues.put(key, previousValue);
            }

            if (contextEntry.isCreated()) {
               cacheNotifier.notifyCacheEntryCreated(key, newValue, metadata, true, ctx, command);
            } else {
               cacheNotifier.notifyCacheEntryModified(key, newValue, metadata, previousValue,
                     previousMetadata, true, ctx, command);
            }

            contextEntry.setValue(newValue);
            Metadatas.updateMetadata(contextEntry, metadata);
            contextEntry.setChanged(true);
         }
      }
      return previousValues;
   }

   private MVCCEntry<Object, Object> lookupMvccEntry(InvocationContext ctx, Object key) {
      //noinspection unchecked
      return (MVCCEntry) ctx.lookupEntry(key);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      visitRemoveCommand(ctx, command, false);
      return null;
   }

   @Override
   public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) throws Throwable {
      Object key = command.getKey();
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      if (e != null && !e.isRemoved()) {
         Object prevValue = e.getValue();
         Object optionalValue = command.getValue();
         Metadata metadata = e.getMetadata();
         Long lifespan = command.getLifespan();
         ValueMatcher valueMatcher = command.getValueMatcher();
         // If the provided lifespan is null, that means it is a store removal command, so we can't compare lifespan
         if (lifespan == null) {
            if (valueMatcher.matches(prevValue, optionalValue, null)) {
               e.setExpired(true);
               return performRemove(e, ctx, valueMatcher, key, prevValue, optionalValue, false, command);
            }
         } else if (metadata == null || metadata.version() == nonExistentVersion) {
            // If there is no metadata and no value that means it is gone currently or not shown due to expired
            // Non existent version is used when versioning is enabled and the entry doesn't exist
            // If we have a value though we should verify it matches the value as well
            if (optionalValue == null || valueMatcher.matches(prevValue, optionalValue, null)) {
               e.setExpired(true);
               return performRemove(e, ctx, valueMatcher, key, prevValue, optionalValue, false, command);
            }
         } else if (e.getLifespan() > 0 && e.getLifespan() == lifespan) {
            // If the entries lifespan is not positive that means it can't expire so don't even try to remove it
            // Lastly if there is metadata we have to verify it equals our lifespan and the value match.
            if (valueMatcher.matches(prevValue, optionalValue, null)) {
               // Only remove the entry if it is still expired.
               // Due to difference in system clocks between nodes - we also accept the expiration within 100 ms
               // of now. This along with the fact that entries are not normally access immediately when they
               // expire should give a good enough buffer range to not create a false positive.
               if (ExpiryHelper.isExpiredMortal(lifespan, e.getCreated(), timeService.wallClockTime() + CLOCK_BUFFER)) {
                  if (trace) {
                     log.tracef("Removing entry as its lifespan and value match and it created on %s with a current time of %s",
                           e.getCreated(), timeService.wallClockTime());
                  }
                  e.setExpired(true);
                  return performRemove(e, ctx, valueMatcher, key, prevValue, optionalValue, false, command);
               } else if (trace) {
                  log.tracef("Cannot remove entry due to it not being expired - this can be caused by different " +
                        "clocks on nodes or a concurrent write");
               }
            }
         } else if (trace) {
            log.trace("Cannot remove entry as its lifespan or value do not match");
         }
      } else if (trace) {
         log.trace("Nothing to remove since the entry doesn't exist in the context or it is already removed");
      }
      command.fail();
      return false;
   }

   private Cache cacheWithFlags(long flagBitSet) {
      if (flagBitSet != EnumUtil.EMPTY_BIT_SET) {
         return unwrappedCache.getAdvancedCache().withFlags(EnumUtil.enumArrayOf(flagBitSet, Flag.class));
      } else {
         return unwrappedCache;
      }
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      // Use an unwrapped cache to avoid unneeded transformations as we only want a count
      long size = cacheWithFlags(command.getFlagsBitSet()).keySet().stream().count();
      if (size > Integer.MAX_VALUE) {
         return Integer.MAX_VALUE;
      } else {
         return (int) size;
      }
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      CacheEntry entry = ctx.lookupEntry(command.getKey());
      if (entry.isRemoved()) {
         if (trace) {
            log.tracef("Entry has been deleted and is of type %s", entry.getClass().getSimpleName());
         }
         return null;
      }

      return entry.getValue();
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      CacheEntry entry = ctx.lookupEntry(command.getKey());
      if (entry.isNull() || entry.isRemoved()) {
         return null;
      }

      // TODO: this shouldn't need a copy when the context is not local
      return internalEntryFactory.copy(entry);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      Map<Object, Object> map = new LinkedHashMap<>();
      for (Object key : command.getKeys()) {
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry == null) {
            throw new IllegalStateException("Entry for key " + toStr(key) + " not found");
         }
         if (entry.isNull()) {
            if (trace) {
               log.tracef("Entry for key %s is null in current context", toStr(key));
            }
            map.put(key, null);
            continue;
         }
         if (entry.isRemoved()) {
            if (trace) {
               log.tracef("Entry for key %s has been deleted and is of type %s", toStr(key), entry.getClass().getSimpleName());
            }
            map.put(key, null);
            continue;
         }

         // Get cache entry instead of value
         if (command.isReturnEntries()) {
            CacheEntry copy;
            if (ctx.isOriginLocal()) {
               copy = internalEntryFactory.copy(entry);
            } else {
               copy = entry;
            }
            if (trace) {
               log.tracef("Found entry %s -> %s", toStr(key), entry);
               log.tracef("Returning copied entry %s", copy);
            }
            map.put(key, copy);
         } else {
            Object value = entry.getValue();
            if (trace) {
               log.tracef("Found %s -> %s", toStr(key), toStr(value));
            }
            map.put(key, value);
         }
      }
      return map;
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      long flagBitSet = command.getFlagsBitSet();
      // We have to check for the flags when we do perform - as interceptor could change this while going down
      // the stack
      boolean isRemoteIteration = EnumUtil.containsAny(flagBitSet, FlagBitSets.REMOTE_ITERATION);
      return new BackingKeySet<>(cacheWithFlags(flagBitSet), dataContainer, keyPartitioner, isRemoteIteration);
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      long flagsBitSet = command.getFlagsBitSet();
      // We have to check for the flags when we do perform - as interceptor could change this while going down
      // the stack
      boolean isRemoteIteration = EnumUtil.containsAny(flagsBitSet, FlagBitSets.REMOTE_ITERATION);
      Cache flagCache = cacheWithFlags(flagsBitSet);
      Object lockOwner = ctx.getLockOwner();
      if (ctx.getLockOwner() != null) {
         return new BackingEntrySet<>(flagCache.getAdvancedCache().lockAs(lockOwner), dataContainer, keyPartitioner, isRemoteIteration);
      }
      return new BackingEntrySet<>(flagCache, dataContainer, keyPartitioner, isRemoteIteration);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      // Nothing to do
      return null;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      // Nothing to do
      return null;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      // Nothing to do
      return null;
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand) throws Throwable {
      Object[] keys = invalidateCommand.getKeys();
      if (trace) {
         log.tracef("Invalidating keys %s", toStr(Arrays.asList(keys)));
      }
      for (Object key : keys) {
         MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
         if (e != null) {
            // TODO: this was null possibly before, what should we do?
            cacheNotifier.notifyCacheEntryInvalidated(key, e.getValue(), e.getMetadata(), true, ctx, invalidateCommand);
            e.setChanged(true);
            e.setRemoved(true);
            e.setCreated(false);
            e.setValid(false);
         }
      }
      return null;
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command invalidateL1Command) throws Throwable {
      Object[] keys = invalidateL1Command.getKeys();
      if (trace) {
         log.tracef("Preparing to invalidate keys %s", Arrays.asList(keys));
      }
      for (Object key : keys) {
         InternalCacheEntry ice = dataContainer.get(key);
         if (ice != null) {
            boolean isLocal = distributionManager.getCacheTopology().isWriteOwner(key);
            if (!isLocal) {
               if (trace) log.tracef("Invalidating key %s.", key);
               MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
               cacheNotifier.notifyCacheEntryInvalidated(key, e.getValue(), e.getMetadata(), true, ctx, invalidateL1Command);
               e.setRemoved(true);
               e.setChanged(true);
               e.setCreated(false);
               e.setValid(false);
            } else {
               log.tracef("Not invalidating key %s as it is local now", key);
            }
         }
      }
      return null;
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      // Nothing to do
      return null;
   }

   @Override
   public Object visitUnknownCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return command.perform(ctx);
   }

   @Override
   public Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      final KeyValueCollector collector = ctx.isOriginLocal() ?
            new LocalContextKeyValueCollector() :
            new RemoteContextKeyValueCollector();
      Object groupName = command.getGroupName();
      ctx.forEachValue((key, entry) -> {
         if (groupName.equals(groupManager.getGroup(key))) {
            collector.addCacheEntry(entry);
         }
      });
      return collector.getResult();
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      if (command instanceof TxReadOnlyKeyCommand) {
         TxReadOnlyKeyCommand txReadOnlyKeyCommand = (TxReadOnlyKeyCommand) command;
         List<Mutation> mutations = txReadOnlyKeyCommand.getMutations();
         if (mutations != null && !mutations.isEmpty()) {
            return visitTxReadOnlyKeyCommand(ctx, txReadOnlyKeyCommand, mutations);
         }
      }
      Object key = command.getKey();
      CacheEntry entry = ctx.lookupEntry(key);

      if (entry == null) {
         throw new IllegalStateException();
      }

      DataConversion keyDataConversion = command.getKeyDataConversion();
      EntryView.ReadEntryView ro = entry.isNull() ? EntryViews.noValue(key, keyDataConversion) :
            EntryViews.readOnly(entry, keyDataConversion, command.getValueDataConversion());
      Object ret = snapshot(command.getFunction().apply(ro));
      // We'll consider the entry always read for stats purposes even if the function is a noop
      return Param.StatisticsMode.isSkip(command.getParams()) ? ret : StatsEnvelope.create(ret, entry.isNull());
   }

   private Object visitTxReadOnlyKeyCommand(InvocationContext ctx, TxReadOnlyKeyCommand command, List<Mutation> mutations) {
      MVCCEntry entry = (MVCCEntry) ctx.lookupEntry(command.getKey());
      EntryView.ReadWriteEntryView rw = EntryViews.readWrite(entry, command.getKeyDataConversion(),
            command.getValueDataConversion());
      Object ret = null;
      for (Mutation mutation : mutations) {
         ret = mutation.apply(rw);
         entry.updatePreviousValue();
      }
      Function function = command.getFunction();
      if (function != null) {
         ret = function.apply(rw);
      }
      ret = snapshot(ret);
      return Param.StatisticsMode.isSkip(command.getParams()) ? ret : StatsEnvelope.create(ret, entry.isNull());
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      if (command instanceof TxReadOnlyManyCommand) {
         TxReadOnlyManyCommand txReadOnlyManyCommand = (TxReadOnlyManyCommand) command;
         List<List<Mutation>> mutations = txReadOnlyManyCommand.getMutations();
         if (mutations != null && !mutations.isEmpty()) {
            return visitTxReadOnlyCommand(ctx, (TxReadOnlyManyCommand) command, mutations);
         }
      }
      Collection<?> keys = command.getKeys();
      // lazy execution triggers exceptions on unexpected places
      ArrayList<Object> retvals = new ArrayList<>(keys.size());
      boolean skipStats = Param.StatisticsMode.isSkip(command.getParams());
      DataConversion keyDataConversion = command.getKeyDataConversion();
      DataConversion valueDataConversion = command.getValueDataConversion();
      Function function = command.getFunction();
      for (Object k : keys) {
         CacheEntry me = ctx.lookupEntry(k);
         EntryView.ReadEntryView view = me.isNull() ?
               EntryViews.noValue(k, keyDataConversion) :
               EntryViews.readOnly(me, keyDataConversion, valueDataConversion);
         Object ret = snapshot(function.apply(view));
         retvals.add(skipStats ? ret : StatsEnvelope.create(ret, me.isNull()));
      }
      return retvals.stream();
   }

   private Object visitTxReadOnlyCommand(InvocationContext ctx, TxReadOnlyManyCommand command, List<List<Mutation>> mutations) {
      Collection<Object> keys = command.getKeys();
      ArrayList<Object> retvals = new ArrayList<>(keys.size());
      Iterator<List<Mutation>> mutIt = mutations.iterator();
      boolean skipStats = Param.StatisticsMode.isSkip(command.getParams());
      Function function = command.getFunction();
      DataConversion keyDataConversion = command.getKeyDataConversion();
      DataConversion valueDataConversion = command.getValueDataConversion();
      for (Object k : keys) {
         List<Mutation> innerMutations = mutIt.next();
         MVCCEntry entry = (MVCCEntry) ctx.lookupEntry(k);
         EntryView.ReadEntryView ro;
         Object ret = null;
         if (mutations.isEmpty()) {
            ro = entry.isNull() ? EntryViews.noValue(k, keyDataConversion) : EntryViews.readOnly(entry, keyDataConversion, valueDataConversion);
         } else {
            EntryView.ReadWriteEntryView rw = EntryViews.readWrite(entry, keyDataConversion, valueDataConversion);
            for (Mutation mutation : innerMutations) {
               ret = mutation.apply(rw);
               entry.updatePreviousValue();
            }
            ro = rw;
         }
         if (function != null) {
            ret = function.apply(ro);
         }
         ret = snapshot(ret);
         retvals.add(skipStats ? ret : StatsEnvelope.create(ret, entry.isNull()));
      }
      return retvals.stream();
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      CacheEntry e = ctx.lookupEntry(command.getKey());

      // Could be that the key is not local
      if (e == null) return null;

      // should we leak this to stats in write-only commands? we do that for events anyway...
      boolean exists = e.getValue() != null;
      command.getConsumer().accept(EntryViews.writeOnly(e, command.getValueDataConversion()));
      // The effective result of retried command is not safe; we'll go to backup anyway
      if (!e.isChanged() && !command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         command.fail();
      }
      return Param.StatisticsMode.isSkip(command.getParams()) ? null : StatsEnvelope.create(null, e, exists, false);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      ValueMatcher valueMatcher = command.getValueMatcher();
      // It's not worth looking up the entry if we're never going to apply the change.
      if (valueMatcher == ValueMatcher.MATCH_NEVER) {
         command.fail();
         return null;
      }

      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(command.getKey());

      // Could be that the key is not local
      if (e == null) return null;

      Object prevValue = command.getPrevValue();
      Metadata prevMetadata = command.getPrevMetadata();
      boolean hasCommandRetry = command.hasAnyFlag(FlagBitSets.COMMAND_RETRY);
      // Command only has one previous value, do not override it
      if (prevValue == null && !hasCommandRetry) {
         prevValue = e.getValue();
         prevMetadata = e.getMetadata();
         command.setPrevValueAndMetadata(prevValue, prevMetadata);
      }

      // Protect against outdated old value using the value matcher.
      // If the value has been update while on the retry, use the newer value.
      // Also take into account that the value might have been removed.
      // TODO: Configure equivalence function
      // TODO: this won't work properly until we store if the command was executed or not...
      Object oldPrevValue = e.getValue();
      // Note: other commands don't clone the entry as they don't carry the previous value for comparison
      // using value matcher - if other commands are retried these can apply the function multiple times.
      // Here we don't want to modify the value in context when trying what would be the outcome of the operation.
      MVCCEntry copy = e.clone();
      DataConversion valueDataConversion = command.getValueDataConversion();
      Object decodedArgument = valueDataConversion.fromStorage(command.getArgument());
      EntryViews.AccessLoggingReadWriteView view = EntryViews.readWrite(copy, prevValue, prevMetadata,
            command.getKeyDataConversion(), valueDataConversion);
      Object ret = snapshot(command.getBiFunction().apply(decodedArgument, view));
      if (valueMatcher.matches(oldPrevValue, prevValue, copy.getValue())) {
         log.tracef("Execute read-write function on previous value %s and previous metadata %s", prevValue, prevMetadata);
         e.setValue(copy.getValue());
         e.setMetadata(copy.getMetadata());
         // These are the only flags that should be changed with EntryViews.readWrite
         e.setChanged(copy.isChanged());
         e.setRemoved(copy.isRemoved());
      }
      // The effective result of retried command is not safe; we'll go to backup anyway
      if (!e.isChanged() && !hasCommandRetry) {
         command.fail();
      }
      return Param.StatisticsMode.isSkip(command.getParams()) ? ret : StatsEnvelope.create(ret, e, prevValue != null, view.isRead());
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      // It's not worth looking up the entry if we're never going to apply the change.
      if (command.getValueMatcher() == ValueMatcher.MATCH_NEVER) {
         command.fail();
         return null;
      }

      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(command.getKey());

      // Could be that the key is not local, 'null' is how this is signalled
      if (e == null) return null;

      Object ret;
      boolean exists = e.getValue() != null;
      EntryViews.AccessLoggingReadWriteView view = EntryViews.readWrite(e, command.getKeyDataConversion(),
            command.getValueDataConversion());
      ret = snapshot(command.getFunction().apply(view));
      // The effective result of retried command is not safe; we'll go to backup anyway
      if (!e.isChanged() && !command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         command.fail();
      }
      return Param.StatisticsMode.isSkip(command.getParams()) ? ret : StatsEnvelope.create(ret, e, exists, view.isRead());
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      Map<Object, Object> arguments = command.getArguments();
      DataConversion valueDataConversion = command.getValueDataConversion();
      for (Map.Entry entry : arguments.entrySet()) {
         CacheEntry cacheEntry = ctx.lookupEntry(entry.getKey());

         // Could be that the key is not local, 'null' is how this is signalled
         if (cacheEntry == null) {
            throw new IllegalStateException();
         }
         Object decodedValue = valueDataConversion.fromStorage(entry.getValue());
         command.getBiConsumer().accept(decodedValue, EntryViews.writeOnly(cacheEntry, valueDataConversion));
      }
      return null;
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      CacheEntry e = ctx.lookupEntry(command.getKey());

      // Could be that the key is not local
      if (e == null) return null;

      DataConversion valueDataConversion = command.getValueDataConversion();
      Object decodedArgument = valueDataConversion.fromStorage(command.getArgument());
      boolean exists = e.getValue() != null;
      command.getBiConsumer().accept(decodedArgument, EntryViews.writeOnly(e, valueDataConversion));
      // The effective result of retried command is not safe; we'll go to backup anyway
      if (!e.isChanged() && !command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         command.fail();
      }
      return Param.StatisticsMode.isSkip(command.getParams()) ? null : StatsEnvelope.create(null, e, exists, false);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      Consumer consumer = command.getConsumer();
      DataConversion valueDataConversion = command.getValueDataConversion();
      for (Object k : command.getAffectedKeys()) {
         CacheEntry cacheEntry = ctx.lookupEntry(k);
         if (cacheEntry == null) {
            throw new IllegalStateException();
         }
         consumer.accept(EntryViews.writeOnly(cacheEntry, valueDataConversion));
      }
      return null;
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      // Can't return a lazy stream here because the current code in
      // EntryWrappingInterceptor expects any changes to be done eagerly,
      // otherwise they're not applied. So, apply the function eagerly and
      // return a lazy stream of the void returns.
      Collection<Object> keys = command.getAffectedKeys();
      List<Object> returns = new ArrayList<>(keys.size());
      boolean skipStats = Param.StatisticsMode.isSkip(command.getParams());
      DataConversion keyDataConversion = command.getKeyDataConversion();
      DataConversion valueDataConversion = command.getValueDataConversion();
      Function function = command.getFunction();
      keys.forEach(k -> {
         MVCCEntry entry = (MVCCEntry) ctx.lookupEntry(k);

         boolean exists = entry.getValue() != null;
         EntryViews.AccessLoggingReadWriteView view = EntryViews.readWrite(entry, keyDataConversion, valueDataConversion);
         Object r = snapshot(function.apply(view));
         returns.add(skipStats ? r : StatsEnvelope.create(r, entry, exists, view.isRead()));
      });
      return returns;
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      Map<Object, Object> arguments = command.getArguments();
      List<Object> returns = new ArrayList<>(arguments.size());
      boolean skipStats = Param.StatisticsMode.isSkip(command.getParams());
      BiFunction biFunction = command.getBiFunction();
      DataConversion keyDataConversion = command.getKeyDataConversion();
      DataConversion valueDataConversion = command.getValueDataConversion();
      arguments.forEach((k, arg) -> {
         MVCCEntry entry = (MVCCEntry) ctx.lookupEntry(k);

         if (entry == null) {
            throw new IllegalStateException();
         }
         Object decodedArgument = valueDataConversion.fromStorage(arg);
         boolean exists = entry.getValue() != null;
         EntryViews.AccessLoggingReadWriteView view = EntryViews.readWrite(entry, keyDataConversion, valueDataConversion);
         Object r = snapshot(biFunction.apply(decodedArgument, view));
         returns.add(skipStats ? r : StatsEnvelope.create(r, entry, exists, view.isRead()));
      });
      return returns;
   }

   static class BackingEntrySet<K, V> extends AbstractCollection<CacheEntry<K, V>> implements CacheSet<CacheEntry<K, V>> {
      private final boolean isRemoteIteration;
      private final Cache<K, V> cache;
      private final InternalDataContainer<K, V> dataContainer;
      private final KeyPartitioner keyPartitioner;

      BackingEntrySet(Cache<K, V> cache, InternalDataContainer<K, V> dataContainer, KeyPartitioner keyPartitioner,
            boolean isRemoteIteration) {
         this.cache = cache;
         this.dataContainer = dataContainer;
         this.keyPartitioner = keyPartitioner;
         this.isRemoteIteration = isRemoteIteration;
      }

      @Override
      public CloseableIterator<CacheEntry<K, V>> iterator() {
         if (isRemoteIteration) {
            // If remote don't do all this wrapping
            return Closeables.iterator(dataContainer.iterator());
         }
         Iterator<CacheEntry<K, V>> iterator = new DataContainerRemoveIterator<>(cache, dataContainer);
         return new IteratorMapper<>(iterator, e -> new EntryWrapper<>(cache, e));
      }

      static <K, V> CloseableSpliterator<CacheEntry<K, V>> closeableCast(Spliterator spliterator) {
         if (spliterator instanceof CloseableSpliterator) {
            return (CloseableSpliterator<CacheEntry<K, V>>) spliterator;
         }
         return Closeables.spliterator((Spliterator<CacheEntry<K, V>>) spliterator);
      }

      @Override
      public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
         // Spliterator doesn't support remove so just return it without wrapping
         return closeableCast(dataContainer.spliterator());
      }

      @Override
      public int size() {
         return dataContainer.size();
      }

      @Override
      public boolean contains(Object o) {
         Map.Entry entry = toEntry(o);
         if (entry != null) {
            InternalCacheEntry<K, V> value = dataContainer.get(entry.getKey());
            return value != null && value.getValue().equals(entry.getValue());
         }
         return false;
      }

      @Override
      public boolean remove(Object o) {
         Map.Entry entry = toEntry(o);
         // Remove must be done by the cache
         return entry != null && cache.remove(entry.getKey(), entry.getValue());
      }

      private Map.Entry<K, V> toEntry(Object obj) {
         if (obj instanceof Map.Entry) {
            return (Map.Entry) obj;
         } else {
            return null;
         }
      }

      private CacheStream<CacheEntry<K, V>> doStream(boolean parallel) {
         return new LocalCacheStream<>(new SegmentedEntryStreamSupplier<>(cache, keyPartitioner, dataContainer),
               parallel, cache.getAdvancedCache().getComponentRegistry());
      }

      @Override
      public CacheStream<CacheEntry<K, V>> stream() {
         return doStream(false);
      }

      @Override
      public CacheStream<CacheEntry<K, V>> parallelStream() {
         return doStream(true);
      }

      @Override
      public Publisher<CacheEntry<K, V>> localPublisher(int segment) {
         // Cast is required since nested generic can't handle sub types properly
         return (Publisher) dataContainer.publisher(segment);

      }

      @Override
      public Publisher<CacheEntry<K, V>> localPublisher(IntSet segments) {
         return new FlowableFromIntSetFunction<>(segments, dataContainer::publisher)
               .flatMap(Functions.identity());
      }
   }

   private static class BackingKeySet<K, V> extends AbstractCollection<K> implements CacheSet<K> {
      private final boolean isRemoteIteration;
      private final Cache<K, V> cache;
      private final InternalDataContainer<K, V> dataContainer;
      private final KeyPartitioner keyPartitioner;

      BackingKeySet(Cache<K, V> cache, InternalDataContainer<K, V> dataContainer, KeyPartitioner keyPartitioner, boolean isRemoteIteration) {
         this.cache = cache;
         this.dataContainer = dataContainer;
         this.keyPartitioner = keyPartitioner;
         this.isRemoteIteration = isRemoteIteration;
      }

      @Override
      public CloseableIterator<K> iterator() {
         if (isRemoteIteration) {
            // Don't add the extra wrapping for removal
            return new IteratorMapper<>(dataContainer.iterator(), Map.Entry::getKey);
         }
         return new IteratorMapper<>(new DataContainerRemoveIterator<>(cache, dataContainer), Map.Entry::getKey);
      }

      @Override
      public CloseableSpliterator<K> spliterator() {
         // Spliterator doesn't support remove so just return it without wrapping
         return new SpliteratorMapper<>(dataContainer.spliterator(), Map.Entry::getKey);
      }

      @Override
      public int size() {
         return dataContainer.size();
      }

      @Override
      public boolean contains(Object o) {
         return dataContainer.containsKey(o);
      }

      @Override
      public boolean remove(Object o) {
         return cache.remove(o) != null;
      }

      private CacheStream<K> doStream(boolean parallel) {
         return new LocalCacheStream<>(new SegmentedKeyStreamSupplier<>(cache, keyPartitioner, dataContainer), parallel,
               cache.getAdvancedCache().getComponentRegistry());
      }

      @Override
      public CacheStream<K> stream() {
         return doStream(false);
      }

      @Override
      public CacheStream<K> parallelStream() {
         return doStream(true);
      }

      @Override
      public Publisher<K> localPublisher(int segment) {
         return Flowable.fromPublisher(dataContainer.publisher(segment))
               .map(RxJavaInterop.entryToKeyFunction());

      }

      @Override
      public Publisher<K> localPublisher(IntSet segments) {
         return new FlowableFromIntSetFunction<>(segments, dataContainer::publisher)
               .flatMap(Functions.identity())
               .map(RxJavaInterop.entryToKeyFunction());
      }
   }

   private interface KeyValueCollector {
      void addCacheEntry(CacheEntry entry);

      Object getResult();
   }

   private static class LocalContextKeyValueCollector implements KeyValueCollector {

      private final Map<Object, Object> map;

      private LocalContextKeyValueCollector() {
         map = new HashMap<>();
      }

      @Override
      public void addCacheEntry(CacheEntry entry) {
         map.put(entry.getKey(), entry.getValue());
      }

      @Override
      public Object getResult() {
         return map;
      }
   }

   private static class RemoteContextKeyValueCollector implements KeyValueCollector {

      private final List<CacheEntry> list;

      private RemoteContextKeyValueCollector() {
         list = new LinkedList<>();
      }

      @Override
      public void addCacheEntry(CacheEntry entry) {
         list.add(entry);
      }

      @Override
      public Object getResult() {
         return list;
      }
   }
}

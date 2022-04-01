package org.infinispan.anchored.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.InternalCacheSet;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.SingleKeyNonTxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.impl.BaseRpcInterceptor;
import org.infinispan.notifications.Listener;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;

/**
 * Fetch the real value from the anchor owner in an anchored cache.
 *
 * @author Dan Berindei
 * @since 11
 */
@Listener
@Scope(Scopes.NAMED_CACHE)
public class AnchoredFetchInterceptor<K, V> extends BaseRpcInterceptor {

   private static final Log log = LogFactory.getLog(AnchoredFetchInterceptor.class);

   @Inject CommandsFactory cf;
   @Inject EntryFactory entryFactory;
   @Inject DistributionManager distributionManager;
   @Inject ComponentRef<Cache<K, V>> cache;
   @Inject AnchorManager anchorManager;

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) {
      return asyncInvokeNext(ctx, command, fetchSingleContextValue(ctx, command, false));
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) {
      return asyncInvokeNext(ctx, command, fetchSingleContextValue(ctx, command, false));
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      return asyncInvokeNext(ctx, command, fetchSingleContextValue(ctx, command, true));
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
      return asyncInvokeNext(ctx, command, fetchSingleContextValue(ctx, command, true));
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
      return asyncInvokeNext(ctx, command, fetchSingleContextValue(ctx, command, true));
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return asyncInvokeNext(ctx, command, fetchAllContextValues(ctx, command, true));
   }

   @Override
   public Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command) {
      return asyncInvokeNext(ctx, command, fetchSingleContextValue(ctx, command, true));
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) {
      return asyncInvokeNext(ctx, command, fetchAllContextValues(ctx, command, false));
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) {
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) {
      // TODO The BackingEntrySet may have to move to an AnchoredBulkInterceptor,
      //  so that it intercepts RemoteMetadata entries loaded from the store
      // TODO LocalPublisherManagerImpl always uses CACHE_MODE_LOCAL, so we can't skip remote lookups
      //  Moving this to an AnchoredBulkInterceptor may also help with that problem
//      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP))
//         return invokeNext(ctx, command);

      return new BackingEntrySet((CacheSet<CacheEntry<K,V>>)  invokeNext(ctx, command));
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) {
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
      return invokeNext(ctx, command);
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) {
      throw new IllegalStateException("Command " + command.getClass().getName() + " is not yet supported");
   }

   private CompletionStage<Void> fetchSingleContextValue(InvocationContext ctx, DataCommand command, boolean isWrite) {
      K key = (K) command.getKey();
      CacheEntry<?, ?> ctxEntry = ((SingleKeyNonTxInvocationContext) ctx).getCacheEntry();

      CompletionStage<CacheEntry<K, V>> stage =
            fetchContextValue(ctx, command, key, ctxEntry, command.getSegment(), isWrite);
      if (stage == null)
         return CompletableFutures.completedNull();

      return stage.thenAccept(externalEntry -> {
         entryFactory.wrapExternalEntry(ctx, key, externalEntry, true, isWrite);
      });
   }

   private CompletionStage<Void> fetchAllContextValues(InvocationContext ctx, FlagAffectedCommand command,
                                                       boolean isWrite) {
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP))
         return CompletableFutures.completedNull();

      AggregateCompletionStage<Void> fetchStage = CompletionStages.aggregateCompletionStage();
      List<CompletionStage<CacheEntry<K, V>>> stages = new ArrayList<>(ctx.lookedUpEntriesCount());
      ctx.forEachEntry((key, ctxEntry) -> {
         // TODO Group keys by anchor location and use ClusteredGetAllCommand
         DistributionInfo distributionInfo = distributionManager.getCacheTopology().getDistribution(key);
         CompletionStage<CacheEntry<K, V>> stage;
         stage = fetchContextValue(ctx, command, (K) key, ctxEntry, distributionInfo.segmentId(), isWrite);
         stages.add(stage);
         if (stage != null) {
            fetchStage.dependsOn(stage);
         }
      });

      return fetchStage.freeze().thenAccept(__ -> {
         // The context iteration order is the same, and each entry has a (possibly null) stage
         Iterator<CompletionStage<CacheEntry<K, V>>> iterator = stages.iterator();
         ctx.forEachEntry((key, ctxEntry) -> {
            CompletionStage<CacheEntry<K, V>> stage = iterator.next();
            if (stage != null) {
               CacheEntry<?, ?> ownerEntry = CompletionStages.join(stage);
               entryFactory.wrapExternalEntry(ctx, key, ownerEntry, true, isWrite);
            }
         });
      });
   }

   private CompletionStage<CacheEntry<K, V>> fetchContextValue(InvocationContext ctx, FlagAffectedCommand command,
                                                               K key, CacheEntry<?, ?> ctxEntry, int segment,
                                                               boolean isWrite) {
      if (ctxEntry.getValue() != null) {
         // The key exists and the anchor location is the local node
         // Note: CacheEntry.isNull() cannot be used, some InternalCacheEntry impls always return false
         return null;
      } else if (ctxEntry.getMetadata() instanceof RemoteMetadata) {
         // The key exists and the anchor location is a remote node
         RemoteMetadata remoteMetadata = (RemoteMetadata) ctxEntry.getMetadata();
         Address keyLocation = remoteMetadata.getAddress();

         if (isWrite && !isLocalModeForced(command)) {
            // Store the location for later, the remote fetch will overwrite the metadata
            // Update the key location if the remote node is no longer available
            Address newLocation = anchorManager.updateLocation(keyLocation);
            ((AnchoredReadCommittedEntry) ctxEntry).setLocation(newLocation);
         }

         DistributionInfo distributionInfo =
               distributionManager.getCacheTopology().getSegmentDistribution(segment);
         // Some writes don't need to fetch the previous value on the backup owners and/or the originator
         if (isWrite && !shouldLoad(ctx, command, distributionInfo)) {
            return null;
         }
         return getRemoteValue(keyLocation, key, segment, isWrite);
      } else {
         // The key does not exist in the cache
         if (isWrite && !isLocalModeForced(command)) {
            // Assign the location now, because AnchoredDistributionInterceptor does not see the RemoteMetadata
            Address currentWriter = anchorManager.getCurrentWriter();
            ((AnchoredReadCommittedEntry) ctxEntry).setLocation(currentWriter);
         }
         return null;
      }
   }

   private CompletionStage<CacheEntry<K, V>> getRemoteValue(Address keyLocation, K key, Integer segment,
                                                            boolean isWrite) {
      ClusteredGetCommand getCommand = cf.buildClusteredGetCommand(key, segment, FlagBitSets.SKIP_OWNERSHIP_CHECK);
      getCommand.setTopologyId(0);
      getCommand.setWrite(isWrite);

      FetchResponseCollector<K, V> collector = new FetchResponseCollector<>(key);
      return rpcManager.invokeCommand(keyLocation, getCommand, collector, rpcManager.getSyncRpcOptions());
   }

   private static class FetchResponseCollector<K, V> extends ValidSingleResponseCollector<CacheEntry<K, V>> {
      private final K key;

      public FetchResponseCollector(K key) {
         this.key = key;
      }

      @Override
      protected CacheEntry<K, V> withValidResponse(Address sender, ValidResponse response) {
         Object responseValue = response.getResponseValue();
         if (responseValue == null) {
            return NullCacheEntry.getInstance();
         } else {
            return ((InternalCacheValue<V>) responseValue).toInternalCacheEntry(key);
         }
      }

      @Override
      protected CacheEntry<K, V> targetNotFound(Address sender) {
         // The entry was lost
         return NullCacheEntry.getInstance();
      }
   }

   private class BackingEntrySet extends InternalCacheSet<CacheEntry<K, V>> {
      protected final CacheSet<CacheEntry<K, V>> next;

      public BackingEntrySet(CacheSet<CacheEntry<K, V>> next) {
         this.next = next;
      }

      @Override
      public Publisher<CacheEntry<K, V>> localPublisher(int segment) {
         return fixPublisher(next.localPublisher(segment), segment);
      }

      @Override
      public Publisher<CacheEntry<K, V>> localPublisher(IntSet segments) {
         return fixPublisher(next.localPublisher(segments), null);
      }

      private Flowable<CacheEntry<K, V>> fixPublisher(Publisher<CacheEntry<K, V>> nextPublisher, Integer segment) {
         return Flowable.fromPublisher(nextPublisher)
                        .groupBy(entry -> entry.getMetadata() instanceof RemoteMetadata)
                        .flatMap(gf -> {
                           if (!gf.getKey()) {
                              return gf;
                           }
                           return gf.concatMapMaybe(incompleteEntry -> fixPublisherEntry(segment, incompleteEntry));
                        }, 2);
      }

      private Maybe<CacheEntry<K, V>> fixPublisherEntry(Integer segment, CacheEntry<K, V> localEntry) {
         // The key exists and the anchor location is a remote node
         RemoteMetadata remoteMetadata = (RemoteMetadata) localEntry.getMetadata();
         Address keyLocation = remoteMetadata.getAddress();

         return Maybe.fromCompletionStage(getRemoteValue(keyLocation, localEntry.getKey(), segment, false));
      }
   }
}

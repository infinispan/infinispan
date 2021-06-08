package org.infinispan.interceptors.locking;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClearCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.impl.ActivationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.util.EntryLoader;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.DataOperationOrderer;
import org.infinispan.util.concurrent.DataOperationOrderer.Operation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * ClusteringDependentLogic that orders write operations using the {@link DataOperationOrderer} component.
 */
@Scope(Scopes.NAMED_CACHE)
public class OrderedClusteringDependentLogic implements ClusteringDependentLogic {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final ClusteringDependentLogic cdl;
   private final boolean passivation;

   @Inject
   DataOperationOrderer orderer;
   @Inject ActivationManager activationManager;
   @Inject InternalDataContainer dataContainer;
   @Inject KeyPartitioner keyPartioner;
   @Inject ComponentRegistry componentRegistry;

   @Start
   public void start() {
      componentRegistry.wireDependencies(cdl);
      cdl.start();
   }

   public OrderedClusteringDependentLogic(ClusteringDependentLogic cdl, boolean passivation) {
      this.cdl = cdl;
      this.passivation = passivation;
   }

   @Override
   public LocalizedCacheTopology getCacheTopology() {
      return cdl.getCacheTopology();
   }

   @Override
   public CompletionStage<Void> commitEntry(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx,
                                            Flag trackFlag, boolean l1Invalidation) {

      if (entry instanceof ClearCacheEntry) {
         return cdl.commitEntry(entry, command, ctx, trackFlag, l1Invalidation);
      }

      if (passivation) {
         return commitEntryPassivation(entry, command, ctx, trackFlag, l1Invalidation);
      } else {
         return commitEntryOrdered(entry, command, ctx, trackFlag, l1Invalidation);
      }
   }

   private CompletionStage<Void> commitEntryOrdered(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx,
                                                    Flag trackFlag, boolean l1Invalidation) {
      Object key = entry.getKey();
      CompletableFuture<Operation> ourFuture = new CompletableFuture<>();
      // If this future is null it means there is another pending read/write/eviction for this key, thus
      // we have to wait on it before performing our commit to ensure data is updated properly
      CompletionStage<Operation> waitingFuture = orderer.orderOn(key, ourFuture);

      CompletionStage<Void> chainedStage;
      // We have to wait on another operation to complete before doing the update
      if (waitingFuture != null) {
         chainedStage = waitingFuture.thenCompose(ignore -> cdl.commitEntry(entry, command, ctx, trackFlag, l1Invalidation));
      } else {
         chainedStage = cdl.commitEntry(entry, command, ctx, trackFlag, l1Invalidation);
      }
      // After everything is done we have to make sure to complete our future
      return chainedStage.whenComplete((ignore, ignoreT) -> orderer.completeOperation(key, ourFuture, operation(entry)));
   }


   private CompletionStage<Void> commitEntryPassivation(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx,
                                                        Flag trackFlag, boolean l1Invalidation) {
      // To clarify the below section these operations must be done in order and cannot be be reordered otherwise
      // it can cause data guarantee issues with other operations
      // 1. Acquire the order guarantee via orderer.orderOn
      // 2. Query the data container if the entry is in memory
      // 3. Update the in memory contents
      // 4. Remove the entry from the store if the entry was not in memory
      // 5. Complete/release the order guarantee

      Object key = entry.getKey();
      int segment = SegmentSpecificCommand.extractSegment(command, key, keyPartioner);
      CompletableFuture<Operation> ourFuture = new CompletableFuture<>();
      // If this future is not null it means there is another pending read/write/eviction for this key, thus
      // we have to wait on it before performing our commit to ensure data is updated properly
      CompletionStage<Operation> waitingFuture = orderer.orderOn(key, ourFuture);
      // We don't want to waste time removing an entry from the store if it is in the data container
      // We use peek here instead of containsKey as the value could be expired - if so we want to make sure
      // passivation manager knows the key is not in the store

      CompletionStage<Void> chainedStage;
      if (waitingFuture != null) {
         // We have to wait on another operation to complete before doing the update
         chainedStage = waitingFuture.thenCompose(ignore -> activateKey(key, segment, entry, command, ctx, trackFlag, l1Invalidation));
      } else {
         chainedStage = activateKey(key, segment, entry, command, ctx, trackFlag, l1Invalidation);
      }
      // After everything is done we have to make sure to complete our future
      if (CompletionStages.isCompletedSuccessfully(chainedStage)) {
         orderer.completeOperation(key, ourFuture, operation(entry));
         return CompletableFutures.completedNull();
      } else {
         return chainedStage.whenComplete((ignore, ignoreT) -> orderer.completeOperation(key, ourFuture, operation(entry)));
      }
   }

   private CompletionStage<Void> activateKey(Object key, int segment, CacheEntry entry, FlagAffectedCommand command,
         InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
      // If entry wasn't in container we should activate to remove from store
      boolean shouldActivate = dataContainer.peek(segment, key) == null;
      CompletionStage<Void> commitStage =  cdl.commitEntry(entry, command, ctx, trackFlag, l1Invalidation);
      if (shouldActivate) {
         return commitStage.thenCompose(ignore1 -> {
            if (log.isTraceEnabled()) {
               log.tracef("Activating entry for key %s due to update in dataContainer", key);
            }
            return activationManager.activateAsync(key, segment);
         });
      } else if (log.isTraceEnabled()) {
         log.tracef("Skipping removal from store as %s was in the data container", key);
      }
      return commitStage;
   }

   private static Operation operation(CacheEntry entry) {
      return entry.isRemoved() ? Operation.REMOVE : Operation.WRITE;
   }

   @Override
   public Commit commitType(FlagAffectedCommand command, InvocationContext ctx, int segment, boolean removed) {
      return cdl.commitType(command, ctx, segment, removed);
   }

   @Override
   public CompletionStage<Map<Object, IncrementableEntryVersion>> createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator,
                                                                                                         TxInvocationContext context,
                                                                                                         VersionedPrepareCommand prepareCommand) {
      return cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, context, prepareCommand);
   }

   @Override
   public Address getAddress() {
      return cdl.getAddress();
   }

   @Override
   public <K, V> EntryLoader<K, V> getEntryLoader() {
      return cdl.getEntryLoader();
   }
}

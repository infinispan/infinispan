package org.infinispan.interceptors.impl;

import static org.infinispan.util.IracUtils.getIracVersionFromCacheEntry;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.irac.IracMetadataRequestCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * Interceptor used by IRAC for pessimistic transactional caches to handle the local site updates.
 * <p>
 * On each successful write, a request is made to the primary owner to generate a new {@link IracMetadata}. At this
 * moment, the lock is acquired so no other transaction can change the key.
 * <p>
 * On prepare, the transaction originator waits for all the replies made during the transaction running, sets them in
 * the {@link WriteCommand} and sends the {@link PrepareCommand} to all the owners.
 * <p>
 * The owners only have to retrieve the {@link IracMetadata} from the {@link WriteCommand} and store it.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class PessimisticTxIracLocalInterceptor extends AbstractIracLocalSiteInterceptor {

   private static final IracMetadataResponseCollector RESPONSE_COLLECTOR = new IracMetadataResponseCollector();

   @Inject CommandsFactory commandsFactory;
   @Inject RpcManager rpcManager;

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      return command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ) ?
            visitNonTxDataWriteCommand(ctx, command) :
            visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      if (ctx.isOriginLocal()) {
         return onLocalPrepare(asLocalTxInvocationContext(ctx), command);
      } else {
         //noinspection unchecked
         return onRemotePrepare(ctx, command);
      }
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
      //nothing extra to be done for rollback.
      return invokeNext(ctx, command);
   }

   private Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      final Object key = command.getKey();
      if (isIracState(command)) {
         setMetadataToCacheEntry(ctx.lookupEntry(key), command.getSegment(), command.getInternalMetadata(key).iracMetadata());
         return invokeNext(ctx, command);
      }
      return skipCommand(ctx, command) ?
            invokeNext(ctx, command) :
            invokeNextThenAccept(ctx, command, this::afterVisitDataWriteCommand);
   }

   private void afterVisitDataWriteCommand(InvocationContext ctx, DataWriteCommand command, Object rv) {
      if (!command.isSuccessful()) {
         return;
      }

      // at this point, the primary owner has the lock acquired
      // we send a request for new IracMetadata
      // Only wait for the reply in prepare.
      setMetadataForWrite(asLocalTxInvocationContext(ctx), command, command.getKey());
   }


   private Object visitWriteCommand(InvocationContext ctx, WriteCommand command) {
      return skipCommand(ctx, command) ?
            invokeNext(ctx, command) :
            invokeNextThenAccept(ctx, command, this::afterVisitWriteCommand);
   }

   private void afterVisitWriteCommand(InvocationContext ctx, WriteCommand command, Object rv) {
      if (!command.isSuccessful()) {
         return;
      }
      // at this point, the primary owner has the lock acquired
      // we send a request for new IracMetadata
      // Only wait for the reply in prepare.
      LocalTxInvocationContext txCtx = asLocalTxInvocationContext(ctx);
      for (Object key : command.getAffectedKeys()) {
         setMetadataForWrite(txCtx, command, key);
      }
   }

   private Object onLocalPrepare(LocalTxInvocationContext ctx, PrepareCommand command) {
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] On local prepare for tx %s", command.getGlobalTransaction());
      }

      //on prepare, we need to wait for all replies from the primary owners that contains the new IracMetadata
      //this is required because pessimistic transactions commits in 1 phase!
      AggregateCompletionStage<Void> allStages = CompletionStages.aggregateCompletionStage();
      Iterator<StreamData> iterator = streamKeysFromModifications(command.getModifications()).iterator();
      while (iterator.hasNext()) {
         StreamData data = iterator.next();
         CompletionStage<IracMetadata> rsp = ctx.getIracMetadata(data.key);
         allStages.dependsOn(rsp.thenAccept(iracMetadata -> setMetadataBeforeSendingPrepare(ctx, data, iracMetadata)));
      }
      return asyncInvokeNext(ctx, command, allStages.freeze());
   }

   private Object onRemotePrepare(TxInvocationContext<RemoteTransaction> ctx, PrepareCommand command) {
      //on remote side, we need to merge the irac metadata from the WriteCommand to CacheEntry
      Iterator<StreamData> iterator = streamKeysFromModifications(command.getModifications())
            .filter(this::isWriteOwner)
            .iterator();
      while (iterator.hasNext()) {
         StreamData data = iterator.next();
         setMetadataToCacheEntry(ctx.lookupEntry(data.key), data.segment, data.command.getInternalMetadata(data.key).iracMetadata());
      }
      return invokeNext(ctx, command);
   }

   private void setMetadataBeforeSendingPrepare(LocalTxInvocationContext ctx, StreamData data, IracMetadata metadata) {
      CacheEntry<?, ?> entry = ctx.lookupEntry(data.key);
      assert entry != null;
      updateCommandMetadata(data.key, data.command, metadata);
      if (isWriteOwner(data)) {
         setMetadataToCacheEntry(entry, data.segment, metadata);
      }
   }

   private void setMetadataForWrite(LocalTxInvocationContext ctx, WriteCommand command, Object key) {
      if (ctx.hasIracMetadata(key)) {
         return;
      }
      int segment = getSegment(command, key);
      CompletionStage<IracMetadata> metadata = requestNewMetadata(segment, ctx.lookupEntry(key));
      ctx.storeIracMetadata(key, metadata);
   }

   private CompletionStage<IracMetadata> requestNewMetadata(int segment, CacheEntry<?, ?> cacheEntry) {
      LocalizedCacheTopology cacheTopology = getCacheTopology();
      DistributionInfo dInfo = cacheTopology.getSegmentDistribution(segment);
      if (dInfo.isPrimary()) {
         IracEntryVersion versionSeen = getIracVersionFromCacheEntry(cacheEntry);
         return CompletableFuture.completedFuture(iracVersionGenerator.generateNewMetadata(segment, versionSeen));
      } else {
         return requestNewMetadataFromPrimaryOwner(dInfo, cacheTopology.getTopologyId(), cacheEntry);
      }
   }

   private CompletionStage<IracMetadata> requestNewMetadataFromPrimaryOwner(DistributionInfo dInfo, int topologyId,
                                                                            CacheEntry<?, ?> cacheEntry) {
      IracEntryVersion versionSeen = getIracVersionFromCacheEntry(cacheEntry);
      IracMetadataRequestCommand cmd = commandsFactory.buildIracMetadataRequestCommand(dInfo.segmentId(), versionSeen);
      cmd.setTopologyId(topologyId);
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
      return rpcManager.invokeCommand(dInfo.primary(), cmd, RESPONSE_COLLECTOR, rpcOptions);
   }

   private boolean skipCommand(InvocationContext ctx, FlagAffectedCommand command) {
      return !ctx.isOriginLocal() || command.hasAnyFlag(FlagBitSets.IRAC_UPDATE);
   }

   private static class IracMetadataResponseCollector extends ValidSingleResponseCollector<IracMetadata> {

      @Override
      protected IracMetadata withValidResponse(Address sender, ValidResponse response) {
         Object rv = response.getResponseValue();
         assert rv instanceof IracMetadata : "[IRAC] invalid response! Expects IracMetadata but got " + rv;
         return (IracMetadata) rv;
      }

      @Override
      protected IracMetadata targetNotFound(Address sender) {
         throw ResponseCollectors.remoteNodeSuspected(sender);
      }
   }
}

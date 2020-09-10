package org.infinispan.interceptors.impl;

import static org.infinispan.remoting.responses.PrepareResponse.asPrepareResponse;
import static org.infinispan.util.IracUtils.getIracVersionFromCacheEntry;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.responses.PrepareResponse;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor used by IRAC for optimistic transactional caches to handle the local site updates.
 * <p>
 * On prepare, if successful, the primary owners generate the {@link IracMetadata} to commit and send it back to the
 * transaction originator. When committing, the {@link IracMetadata} is set in the context entries to be stored.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class OptimisticTxIracLocalSiteInterceptor extends AbstractIracLocalSiteInterceptor {

   private static final Log log = LogFactory.getLog(OptimisticTxIracLocalSiteInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private final InvocationSuccessAction<PrepareCommand> afterLocalPrepare = this::afterLocalTwoPhasePrepare;
   private final InvocationSuccessFunction<PrepareCommand> afterRemotePrepare = this::afterRemoteTwoPhasePrepare;

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      final Object key = command.getKey();
      if (isIracState(command)) {
         // if this is a state transfer from a remote site, we set the versions here
         setMetadataToCacheEntry(ctx.lookupEntry(key), command.getInternalMetadata(key).iracMetadata());
      }
      return invokeNext(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      //note: both methods ignore PutKeyValueCommand from state transfer. That's why the IracMetadata is set above!
      // if the prepare fails, (exception) the methods aren't invoked.
      if (ctx.isOriginLocal()) {
         return invokeNextThenAccept(ctx, command, afterLocalPrepare);
      } else {
         return invokeNextThenApply(ctx, command, afterRemotePrepare);
      }
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      if (ctx.isOriginLocal()) {
         return onLocalCommitCommand(ctx, command);
      } else {
         return onRemoteCommitCommand(ctx, command);
      }
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
      //nothing extra to be done for rollback.
      return invokeNext(ctx, command);
   }

   @Override
   public boolean isTraceEnabled() {
      return trace;
   }

   @Override
   public Log getLog() {
      return log;
   }

   private void afterLocalTwoPhasePrepare(InvocationContext ctx, PrepareCommand command, Object rv) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] After successful local prepare for tx %s. Return Value: %s",
               command.getGlobalTransaction(), rv);
      }
      PrepareResponse prepareResponse = asPrepareResponse(rv);
      Iterator<StreamData> iterator = streamKeysFromModifications(command.getModifications()).iterator();
      Map<Integer, IracMetadata> segmentMetadata = new HashMap<>();
      while (iterator.hasNext()) {
         StreamData data = iterator.next();
         IracMetadata metadata;
         if (isPrimaryOwner(data)) {
            IracEntryVersion versionSeen = getIracVersionFromCacheEntry(ctx.lookupEntry(data.key));
            metadata = segmentMetadata.computeIfAbsent(data.segment, segment -> iracVersionGenerator.generateNewMetadata(segment, versionSeen));
         } else {
            metadata = segmentMetadata.computeIfAbsent(data.segment, prepareResponse::getIracMetadata);
         }
         assert metadata != null : "[IRAC] metadata is null after successful prepare! Data=" + data;
         updateCommandMetadata(data.key, data.command, metadata);
      }
   }

   private Object afterRemoteTwoPhasePrepare(InvocationContext ctx, PrepareCommand command, Object rv) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] After successful remote prepare for tx %s. Return Value: %s",
               command.getGlobalTransaction(), rv);
      }
      PrepareResponse rsp = PrepareResponse.asPrepareResponse(rv);
      Iterator<StreamData> iterator = streamKeysFromModifications(command.getModifications())
            .filter(this::isPrimaryOwner)
            .distinct()
            .iterator();
      Map<Integer, IracEntryVersion> maxVersionSeen = new HashMap<>();
      while (iterator.hasNext()) {
         StreamData data = iterator.next();
         IracEntryVersion versionSeen = getIracVersionFromCacheEntry(ctx.lookupEntry(data.key));
         if (versionSeen != null) {
            maxVersionSeen.merge(data.segment, versionSeen, IracEntryVersion::merge);
         } else {
            maxVersionSeen.putIfAbsent(data.segment, null);
         }
      }
      Map<Integer, IracMetadata> segmentMetadata = new HashMap<>();
      maxVersionSeen.forEach((segment, version) ->
            segmentMetadata.put(segment, iracVersionGenerator.generateNewMetadata(segment, version)));
      rsp.setNewIracMetadata(segmentMetadata);
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] After successful remote prepare for tx %s. New Return Value: %s",
               command.getGlobalTransaction(), rsp);
      }
      return rsp;
   }

   private Object onLocalCommitCommand(TxInvocationContext<?> ctx, CommitCommand command) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] On local Commit for tx %s", command.getGlobalTransaction());
      }
      Iterator<StreamData> iterator = streamKeysFromModifications(ctx.getModifications()).iterator();
      while (iterator.hasNext()) {
         StreamData data = iterator.next();
         IracMetadata metadata = data.command.getInternalMetadata(data.key).iracMetadata();

         command.addIracMetadata(data.segment, metadata);
         if (isWriteOwner(data)) {
            setMetadataToCacheEntry(ctx.lookupEntry(data.key), metadata);
         }
      }
      return invokeNext(ctx, command);
   }

   private Object onRemoteCommitCommand(TxInvocationContext<?> context, CommitCommand command) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] On remote Commit for tx %s", command.getGlobalTransaction());
      }
      RemoteTxInvocationContext ctx = asRemoteTxInvocationContext(context);
      Iterator<StreamData> iterator = streamKeysFromModifications(ctx.getModifications())
            .filter(this::isWriteOwner)
            .iterator();
      while (iterator.hasNext()) {
         StreamData data = iterator.next();
         IracMetadata metadata = command.getIracMetadata(data.segment);
         setMetadataToCacheEntry(ctx.lookupEntry(data.key), metadata);
      }
      return invokeNext(ctx, command);
   }

   private Stream<StreamData> streamKeysFromModifications(List<WriteCommand> mods) {
      return streamKeysFromModifications(mods.stream());
   }
}

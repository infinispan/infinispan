package org.infinispan.anchored.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.interceptors.distribution.WriteManyCommandHelper;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link NonTxDistributionInterceptor} replacement for anchored caches.
 *
 * <p>The interceptor behaves mostly like {@link NonTxDistributionInterceptor},
 * but when the primary sends the command to the backups it only sends the actual value
 * to the key's anchor owner.</p>
 *
 * <p>If the key is new, the anchor owner is the last member of the read CH.
 * If the key already exists in the cache, the anchor owner is preserved.</p>
 *
 * @author Dan Berindei
 * @since 11
 */
public class AnchoredDistributionInterceptor extends NonTxDistributionInterceptor {
// TODO Investigate extending TriangleDistributionInterceptor instead of NonTxDistributionInterceptor

   private static final Log log = LogFactory.getLog(AnchoredDistributionInterceptor.class);

   @Inject CommandsFactory commandsFactory;
   @Inject AnchorManager anchorManager;

   @Override
   protected Object primaryReturnHandler(InvocationContext ctx, AbstractDataWriteCommand command, Object localResult) {
      if (!command.isSuccessful()) {
         if (log.isTraceEnabled()) log.tracef(
               "Skipping the replication of the conditional command as it did not succeed on primary owner (%s).",
               command);
         return localResult;
      }
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      DistributionInfo distributionInfo = cacheTopology.getSegmentDistribution(command.getSegment());

      Collection<Address> owners = distributionInfo.writeOwners();
      if (owners.size() == 1) {
         // There are no backups, skip the replication part.
         return localResult;
      }

      // Match always on the backups, but save the original matcher for retries
      ValueMatcher originalMatcher = command.getValueMatcher();
      command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);

      CommandCopier commandCopier = new CommandCopier(ctx, command);
      // Ignore the previous value on the backup owners
      assert isSynchronous(command);
      MapResponseCollector collector = MapResponseCollector.ignoreLeavers(isReplicated, owners.size());
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
      CompletionStage<Map<Address, Response>> remoteInvocation =
            rpcManager.invokeCommands(distributionInfo.writeBackups(), commandCopier, collector, rpcOptions);
      return asyncValue(remoteInvocation.handle((responses, t) -> {
         // Switch to the retry policy, in case the primary owner changed
         // and the write already succeeded on the new primary
         command.setValueMatcher(originalMatcher.matcherForRetry());
         CompletableFutures.rethrowExceptionIfPresent(t instanceof RemoteException ? t.getCause() : t);
         return localResult;
      }));
   }

   @Override
   protected <C extends WriteCommand, Container, Item>
   Object handleReadWriteManyCommand(InvocationContext ctx, C command,
                                     WriteManyCommandHelper<C, Item, Container> helper) throws Exception {
      WriteManyCommandHelper wrappedHelper = new AbstractDelegatingWriteManyCommandHelper(helper) {
         @Override
         public WriteCommand copyForBackup(WriteCommand cmd, LocalizedCacheTopology topology, Address target,
                                           IntSet segments) {
            WriteCommand backupCommand = helper.copyForBackup(cmd, topology, target, segments);
            CommandCopier commandCopier = new CommandCopier(ctx, backupCommand);
            return (WriteCommand) commandCopier.apply(target);
         }
      };
      return super.handleReadWriteManyCommand(ctx, command, wrappedHelper);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (command.isForwarded()) {
         assert command.getMetadata() == null || command.getMetadata().version() == null;

         HashMap<Object, Object> valueMap = new HashMap<>();
         for (Map.Entry<?, ?> entry : command.getMap().entrySet()) {
            Object key = entry.getKey();
            CacheEntry ctxEntry = ctx.lookupEntry(key);
            if (ctxEntry != null && entry.getValue() instanceof RemoteMetadata) {
               RemoteMetadata entryMetadata = (RemoteMetadata) entry.getValue();
               ctxEntry.setMetadata(entryMetadata);
               valueMap.put(key, null);
            } else {
               valueMap.put(key, entry.getValue());
            }
         }
         command.setMap(valueMap);
      }
      return super.visitPutMapCommand(ctx, command);
   }

   Address getKeyWriter(CacheEntry<?, ?> contextEntry) {
      // Use the existing location, or the one set by AnchoredFetchInterceptor
      Address location = ((AnchoredReadCommittedEntry) contextEntry).getLocation();
      return location != null ? location : rpcManager.getAddress();
   }

   /**
    * Replaces the value with null and the metadata with a RemoteMetadata instance containing the anchor location.
    *
    * <p>It is used only on the primary owner to copy the command for the backups.</p>
    * <p>Does not replace the value if the target is the anchor location.</p>
    * <p>Assumes the value matcher is already set to MATCH_ALWAYS.</p>
    */
   class CommandCopier extends AbstractVisitor implements Function<Address, ReplicableCommand> {
      private final InvocationContext ctx;
      private final VisitableCommand command;

      private Address target;

      CommandCopier(InvocationContext ctx, VisitableCommand command) {
         this.ctx = ctx;
         this.command = command;
      }

      @Override
      public ReplicableCommand apply(Address address) {
         this.target = address;

         try {
            return (ReplicableCommand) command.acceptVisitor(ctx, this);
         } catch (Throwable throwable) {
            throw new CacheException(throwable);
         }
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         return replaceWithPutRemoteMetadata(ctx, command);
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
         return replaceWithPutRemoteMetadata(ctx, command);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         return command;
      }

      private VisitableCommand replaceWithPutRemoteMetadata(InvocationContext ctx, DataWriteCommand command) {
         Object key = command.getKey();
         Address keyWriter = getKeyWriter(ctx.lookupEntry(key));

         if (target.equals(keyWriter)) {
            // This is the real owner, send the proper value and metadata
            return command;
         }

         Metadata metadata = new RemoteMetadata(keyWriter, null);
         PutKeyValueCommand copy = new PutKeyValueCommand(key, null, false, false, metadata,
                                                          command.getSegment(), command.getFlagsBitSet(),
                                                          command.getCommandInvocationId());
         copy.setValueMatcher(command.getValueMatcher());
         copy.setTopologyId(command.getTopologyId());
         return copy;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
         Map<Object, Object> mapCopy = new HashMap<>(command.getMap().size() * 2);
         for (Map.Entry<Object, Object> entry : command.getMap().entrySet()) {
            Object key = entry.getKey();
            Address keyWriter = getKeyWriter(ctx.lookupEntry(key));
            Metadata metadata = new RemoteMetadata(keyWriter, null);
            if (!target.equals(keyWriter)) {
               // There's a single metadata instance for each PutMapCommand, so we store the location in the value instead
               mapCopy.put(key, metadata);
            } else {
               mapCopy.put(key, entry.getValue());
            }
         }
         // WriteManyCommandHelper already copied the command, so we can modify it in-place
         command.setMap(mapCopy);
         return command;
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) {
         throw new UnsupportedOperationException(
               "Command type " + command.getClass() + " is not yet supported in anchored caches");
      }

   }

}

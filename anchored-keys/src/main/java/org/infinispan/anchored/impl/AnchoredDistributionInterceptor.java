package org.infinispan.anchored.impl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.CacheException;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.notifications.Listener;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.util.concurrent.CompletableFutures;
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
@Listener
public class AnchoredDistributionInterceptor extends NonTxDistributionInterceptor {

   private static Log log = LogFactory.getLog(AnchoredDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject CommandsFactory commandsFactory;
   @Inject AnchorManager anchorManager;

   @Override
   protected Object primaryReturnHandler(InvocationContext ctx, AbstractDataWriteCommand command, Object localResult) {
      assert isReplicated;

      if (!command.isSuccessful()) {
         if (trace) log.tracef(
               "Skipping the replication of the conditional command as it did not succeed on primary owner (%s).",
               command);
         return localResult;
      }
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      DistributionInfo distributionInfo = cacheTopology.getDistribution(command.getKey());
      Collection<Address> owners = distributionInfo.writeOwners();
      if (owners.size() == 1) {
         // There are no backups, skip the replication part.
         return localResult;
      }

      AbstractDataWriteCommand remoteCommand = copyForBackups(ctx, command);
      ValueMatcher originalMatcher = command.getValueMatcher();
      // Ignore the previous value on the backup owners
      remoteCommand.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
      if (!isSynchronous(command)) {
         rpcManager.sendToAll(command, DeliverOrder.PER_SENDER);
         // Switch to the retry policy, in case the primary owner changes before we commit locally
         command.setValueMatcher(originalMatcher.matcherForRetry());
         return localResult;
      }
      MapResponseCollector collector = MapResponseCollector.ignoreLeavers(isReplicated, owners.size());
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
      CompletionStage<Map<Address, Response>> remoteInvocation = rpcManager.invokeCommandOnAll(remoteCommand, collector,
                                                                                               rpcOptions);
      return asyncValue(remoteInvocation.handle((responses, t) -> {
         // Switch to the retry policy, in case the primary owner changed and the write already succeeded on the new
         // primary
         command.setValueMatcher(originalMatcher.matcherForRetry());
         CompletableFutures.rethrowException(t instanceof RemoteException ? t.getCause() : t);
         return localResult;
      }));
   }

   private <T extends VisitableCommand> T copyForBackups(InvocationContext ctx, T command) {
      ReplaceValueVisitor visitor = new ReplaceValueVisitor();
      try {
         T remoteCommand = (T) command.acceptVisitor(ctx, visitor);
         return remoteCommand;
      } catch (Throwable throwable) {
         throw new CacheException(throwable);
      }
   }

   class ReplaceValueVisitor extends AbstractVisitor {

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         Address keyLocation = anchorManager.getCurrentWriter();
         CacheEntry<?, ?> ctxEntry = ctx.lookupEntry(command.getKey());
         if (ctxEntry.getValue() instanceof Address) {
            keyLocation = (Address) ctxEntry.getValue();
         }
         PutKeyValueCommand copy =
               commandsFactory.buildPutKeyValueCommand(command.getKey(), keyLocation, command.getSegment(),
                                                       command.getMetadata(), command.getFlagsBitSet());
         copy.setTopologyId(command.getTopologyId());
         return copy;
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         throw new UnsupportedOperationException(
               "Command type " + command.getClass() + " is not yet supported in anchor caches");
      }
   }
}

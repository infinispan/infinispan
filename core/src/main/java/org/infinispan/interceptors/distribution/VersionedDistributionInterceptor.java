package org.infinispan.interceptors.distribution;

import static org.infinispan.transaction.impl.WriteSkewHelper.readVersionsFromResponse;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A version of the {@link TxDistributionInterceptor} that adds logic to handling prepares when entries are
 * versioned.
 *
 * @author Manik Surtani
 * @author Dan Berindei
 */
public class VersionedDistributionInterceptor extends TxDistributionInterceptor {

   private static final Log log = LogFactory.getLog(VersionedDistributionInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected CompletableFuture<Object> prepareOnAffectedNodes(TxInvocationContext<?> ctx, PrepareCommand command, Collection<Address> recipients) {
      // Perform the RPC
      CompletableFuture<Map<Address, Response>>
            remoteInvocation = rpcManager.invokeRemotelyAsync(recipients, command, createPrepareRpcOptions());
      return remoteInvocation.handle((responses, t) -> {
         transactionRemotelyPrepared(ctx);
         CompletableFutures.rethrowException(t);

         checkTxCommandResponses(responses, command, (TxInvocationContext<LocalTransaction>) ctx, recipients);

         // Now store newly generated versions from lock owners for use during the commit phase.
         CacheTransaction ct = ctx.getCacheTransaction();
         for (Response r : responses.values()) readVersionsFromResponse(r, ct);
         return null;
      });
   }
}

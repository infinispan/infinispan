package org.infinispan.interceptors.distribution;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Map;

import static org.infinispan.transaction.impl.WriteSkewHelper.readVersionsFromResponse;

/**
 * A version of the {@link TxDistributionInterceptor} that adds logic to handling prepares when entries are versioned.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedDistributionInterceptor extends TxDistributionInterceptor {

   private static final Log log = LogFactory.getLog(VersionedDistributionInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command, Collection<Address> recipients, boolean ignored) {
      // Perform the RPC
      try {
         Map<Address, Response> resps = rpcManager.invokeRemotely(recipients, command, rpcManager.getDefaultRpcOptions(true, false));

         // Now store newly generated versions from lock owners for use during the commit phase.
         CacheTransaction ct = ctx.getCacheTransaction();
         for (Response r : resps.values()) readVersionsFromResponse(r, ct);
      } finally {
         transactionRemotelyPrepared(ctx);
      }
   }
}

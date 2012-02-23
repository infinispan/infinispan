package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.VersionedReplicationInterceptor;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.transaction.WriteSkewHelper.setVersionsSeenOnPrepareCommand;

/**
 * Replication Interceptor for Total Order protocol with versioning
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TOVersionedReplicationInterceptor extends VersionedReplicationInterceptor {
   private static final Log log = LogFactory.getLog(TOVersionedReplicationInterceptor.class);

   @Override
   protected void broadcastPrepare(TxInvocationContext context, PrepareCommand command) {
      boolean trace = log.isTraceEnabled();
      String globalTransactionString = Util.prettyPrintGlobalTransaction(command.getGlobalTransaction());

      if(trace) {
         log.tracef("Broadcasting transaction %s with Total Order", globalTransactionString);
      }

      if (!(command instanceof VersionedPrepareCommand)) {
         throw new IllegalStateException("Expected a Versioned Prepare Command in version aware component");
      }

      setVersionsSeenOnPrepareCommand((VersionedPrepareCommand) command, context);
      //broadcast the command
      rpcManager.broadcastRpcCommand(command, false);
   }
}

package org.infinispan.commands.remote.recovery;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Rpc to obtain all in-doubt prepared transactions stored on remote nodes.
 * A transaction is in doubt if it is prepared and the node where it started has crashed.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@ProtoTypeId(ProtoStreamTypeIds.GET_IN_DOUBT_TRANSACTIONS_COMMAND)
public class GetInDoubtTransactionsCommand extends BaseRpcCommand {

   private static final Log log = LogFactory.getLog(GetInDoubtTransactionsCommand.class);

   @ProtoFactory
   public GetInDoubtTransactionsCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      RecoveryManager recoveryManager = componentRegistry.getRecoveryManager().running();
      List<XidImpl> localInDoubtTransactions = recoveryManager.getInDoubtTransactions();
      log.tracef("Returning result %s", localInDoubtTransactions);
      return CompletableFuture.completedFuture(localInDoubtTransactions);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + " { cacheName = " + cacheName + "}";
   }
}

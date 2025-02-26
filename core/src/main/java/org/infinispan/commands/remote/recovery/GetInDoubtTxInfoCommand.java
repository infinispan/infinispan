package org.infinispan.commands.remote.recovery;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.ByteString;

/**
 * Command used by the recovery tooling for obtaining the list of in-doubt transactions from a node.
 *
 * @author Mircea Markus
 * @since 5.0
 */
@ProtoTypeId(ProtoStreamTypeIds.GET_IN_DOUBT_TX_INFO_COMMAND)
public class GetInDoubtTxInfoCommand extends BaseRpcCommand {

   @ProtoFactory
   public GetInDoubtTxInfoCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      RecoveryManager recoveryManager = componentRegistry.getRecoveryManager().running();
      return CompletableFuture.completedFuture(recoveryManager.getInDoubtTransactionInfo());
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

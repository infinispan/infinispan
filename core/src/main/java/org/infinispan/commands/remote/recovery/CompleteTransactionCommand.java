package org.infinispan.commands.remote.recovery;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.ByteString;

/**
 * Command used by the recovery tooling for forcing transaction completion .
 *
 * @author Mircea Markus
 * @since 5.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COMPLETE_TRANSACTION_COMMAND)
public class CompleteTransactionCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 24;

   @ProtoField(number = 2)
   final XidImpl xid;

   @ProtoField(number = 3, defaultValue = "false")
   final boolean commit;

   @ProtoFactory
   public CompleteTransactionCommand(ByteString cacheName, XidImpl xid, boolean commit) {
      super(cacheName);
      this.xid = xid;
      this.commit = commit;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      RecoveryManager recoveryManager = componentRegistry.getRecoveryManager().running();
      return recoveryManager.forceTransactionCompletion(xid, commit);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean canBlock() {
      //this command performs the 2PC commit.
      return true;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{ xid=" + xid +
            ", commit=" + commit +
            ", cacheName=" + cacheName +
            "} ";
   }
}

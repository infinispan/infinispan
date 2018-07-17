package org.infinispan.commands.remote.recovery;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.UserObjectOutput;
import org.infinispan.util.ByteString;

/**
 * Command used by the recovery tooling for obtaining the list of in-doubt transactions from a node.
 *
 * @author Mircea Markus
 * @since 5.0
 */
public class GetInDoubtTxInfoCommand extends RecoveryCommand {

   public static final int COMMAND_ID = 23;

   private GetInDoubtTxInfoCommand() {
      super(null); // For command id uniqueness test
   }

   public GetInDoubtTxInfoCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      return CompletableFuture.completedFuture(recoveryManager.getInDoubtTransactionInfo());
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      // No parameters
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      // No parameters
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + " { cacheName = " + cacheName + "}";
   }

}

package org.infinispan.commands.irac;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.irac.IracManager;

/**
 * A clear request that is sent to the remote site by {@link IracManager}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class IracClearKeysCommand extends IracUpdateKeyCommand<Void> {

   public static final byte COMMAND_ID = 17;

   @SuppressWarnings("unused")
   public IracClearKeysCommand() {
      super(COMMAND_ID, null);
   }

   public IracClearKeysCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public CompletionStage<Void> executeOperation(BackupReceiver receiver) {
      return receiver.clearKeys();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) {
   }

   @Override
   public void readFrom(ObjectInput input) {
   }

   @Override
   public boolean isClear() {
      return true;
   }

   @Override
   public String toString() {
      return "IracClearKeyCommand{" +
            "originSite='" + originSite + '\'' +
            ", cacheName=" + cacheName +
            '}';
   }
}

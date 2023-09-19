package org.infinispan.xsite.commands.remote;

import java.util.concurrent.CompletionStage;

import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.irac.IracManager;

/**
 * A clear request that is sent to the remote site by {@link IracManager}.
 *
 * @author Pedro Ruivo
 * @since 15.0
 */
public class IracClearKeysRequest extends IracUpdateKeyRequest<Void> {


   public IracClearKeysRequest() {
      super(null);
   }

   public IracClearKeysRequest(ByteString cacheName) {
      super(cacheName);
   }

   public CompletionStage<Void> executeOperation(BackupReceiver receiver) {
      return receiver.clearKeys();
   }

   @Override
   public byte getCommandId() {
      return Ids.IRAC_CLEAR;
   }

   @Override
   public String toString() {
      return "IracClearKeyCommand{" +
            ", cacheName=" + cacheName +
            '}';
   }
}

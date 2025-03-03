package org.infinispan.xsite.commands.remote;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.irac.IracManager;

/**
 * A clear request that is sent to the remote site by {@link IracManager}.
 *
 * @author Pedro Ruivo
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_CLEAR_KEYS_COMMAND)
public class IracClearKeysRequest extends IracUpdateKeyRequest<Void> {


   @ProtoFactory
   public IracClearKeysRequest(ByteString cacheName) {
      super(cacheName);
   }

   public CompletionStage<Void> executeOperation(BackupReceiver receiver) {
      return receiver.clearKeys();
   }

   @Override
   public String toString() {
      return "IracClearKeyCommand{" +
            ", cacheName=" + cacheName +
            '}';
   }
}

package org.infinispan.xsite.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.irac.IracManager;

/**
 * A request that is sent to the remote site by {@link IracManager}.
 *
 * @author William Burns
 * @since 15.0
 */
public class IracTouchKeyRequest extends IracUpdateKeyRequest<Boolean> {

   private Object key;

   public IracTouchKeyRequest() {
      super(null);
   }

   public IracTouchKeyRequest(ByteString cacheName, Object key) {
      super(cacheName);
      this.key = key;
   }

   @Override
   public CompletionStage<Boolean> executeOperation(BackupReceiver receiver) {
      return receiver.touchEntry(key);
   }

   @Override
   public byte getCommandId() {
      return Ids.IRAC_TOUCH;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      super.writeTo(output);
   }

   @Override
   public XSiteRequest<Boolean> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      return super.readFrom(input);
   }
}

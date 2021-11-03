package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.irac.IracManager;

/**
 * A remove key request that is sent to the remote site by {@link IracManager}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class IracRemoveKeyCommand extends IracUpdateKeyCommand {

   public static final byte COMMAND_ID = 15;

   private Object key;
   private IracMetadata iracMetadata;
   private boolean expiration;

   @SuppressWarnings("unused")
   public IracRemoveKeyCommand() {
      super(COMMAND_ID, null);
   }

   public IracRemoveKeyCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public IracRemoveKeyCommand(ByteString cacheName, Object key, IracMetadata iracMetadata, boolean expiration) {
      super(COMMAND_ID, cacheName);
      this.key = key;
      this.iracMetadata = iracMetadata;
      this.expiration = expiration;
   }

   public CompletionStage<Void> executeOperation(BackupReceiver receiver) {
      return receiver.removeKey(key, iracMetadata, expiration);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      IracMetadata.writeTo(output, iracMetadata);
      output.writeBoolean(expiration);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.key = input.readObject();
      this.iracMetadata = IracMetadata.readFrom(input);
      this.expiration = input.readBoolean();
   }

   @Override
   public String toString() {
      return "IracRemoveKeyCommand{" +
            "key=" + key +
            ", iracMetadata=" + iracMetadata +
            ", originSite='" + originSite + '\'' +
            ", cacheName=" + cacheName +
            '}';
   }
}

package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.irac.IracManager;

/**
 * A put key request that is sent to the remote site by {@link IracManager}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class IracPutKeyCommand extends IracUpdateKeyCommand {

   public static final byte COMMAND_ID = 123;

   private Object key;
   private Object value;
   private Metadata metadata;
   private IracMetadata iracMetadata;

   @SuppressWarnings("unused")
   public IracPutKeyCommand() {
      super(COMMAND_ID, null);
   }

   public IracPutKeyCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public IracPutKeyCommand(ByteString cacheName, Object key, Object value, Metadata metadata,
         IracMetadata iracMetadata) {
      super(COMMAND_ID, cacheName);
      this.key = key;
      this.value = value;
      this.metadata = metadata;
      this.iracMetadata = iracMetadata;
   }

   public CompletionStage<Void> executeOperation(BackupReceiver receiver) {
      return receiver.putKeyValue(key, value, metadata, iracMetadata);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(value);
      output.writeObject(metadata);
      IracMetadata.writeTo(output, iracMetadata);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.key = input.readObject();
      this.value = input.readObject();
      this.metadata = (Metadata) input.readObject();
      this.iracMetadata = IracMetadata.readFrom(input);
   }

   @Override
   public String toString() {
      return "IracPutKeyCommand{" +
            "key=" + key +
            ", value=" + value +
            ", metadata=" + metadata +
            ", iracMetadata=" + iracMetadata +
            ", originSite='" + originSite + '\'' +
            ", cacheName=" + cacheName +
            '}';
   }
}

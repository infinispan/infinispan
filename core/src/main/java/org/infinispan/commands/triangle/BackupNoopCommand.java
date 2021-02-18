package org.infinispan.commands.triangle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.util.ByteString;

/**
 * A command that tell a backup owner to ignore a sequence id after the primary failed to send a regular write command.
 *
 * @author Dan Berindei
 * @since 12.1
 */
public class BackupNoopCommand extends BackupWriteCommand {

   public static final byte COMMAND_ID = 81;
   //for testing
   @SuppressWarnings("unused")
   public BackupNoopCommand() {
      super(null);
   }

   public BackupNoopCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public void setWriteCommand(WriteCommand command) {
      super.setCommonAttributesFromCommand(command);
   }


   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      writeBase(output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      readBase(input);
   }

   @Override
   public String toString() {
      return "BackupNoopCommand{" + toStringFields() + '}';
   }

   @Override
   WriteCommand createWriteCommand() {
      return null;
   }

   @Override
   String toStringFields() {
      return super.toStringFields();
   }
}

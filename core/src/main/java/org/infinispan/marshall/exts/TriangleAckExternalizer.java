package org.infinispan.marshall.exts;

import static org.infinispan.commons.marshall.Ids.TRIANGLE_ACK_EXTERNALIZER;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.util.ByteString;

/**
 * Externalizer for the triangle acknowledges.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleAckExternalizer implements AdvancedExternalizer<CacheRpcCommand> {

   private final MarshalledEntryFactory entryFactory;

   public TriangleAckExternalizer(MarshalledEntryFactory entryFactory) {
      this.entryFactory = entryFactory;
   }

   public Set<Class<? extends CacheRpcCommand>> getTypeClasses() {
      //noinspection unchecked
      return Util.asSet(BackupAckCommand.class, ExceptionAckCommand.class, BackupMultiKeyAckCommand.class);
   }

   public Integer getId() {
      return TRIANGLE_ACK_EXTERNALIZER;
   }

   public void writeObject(ObjectOutput output, CacheRpcCommand object) throws IOException {
      output.writeByte(object.getCommandId());
      ByteString.writeObject(output, object.getCacheName());
      // TODO add UserAwareObjectOutput instance
      object.writeTo(output);
   }

   public CacheRpcCommand readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      switch (input.readByte()) {
         case BackupAckCommand.COMMAND_ID:
            return backupAckCommand(input);
         case ExceptionAckCommand.COMMAND_ID:
            return exceptionAckCommand(input);
         case BackupMultiKeyAckCommand.COMMAND_ID:
            return backupMultiKeyAckCommand(input);
         default:
            throw new IllegalStateException();
      }
   }

   private BackupMultiKeyAckCommand backupMultiKeyAckCommand(ObjectInput input)
         throws IOException, ClassNotFoundException {
      BackupMultiKeyAckCommand command = new BackupMultiKeyAckCommand(ByteString.readObject(input));
      command.readFrom(input);
      return command;
   }

   private ExceptionAckCommand exceptionAckCommand(ObjectInput input) throws IOException, ClassNotFoundException {
      ExceptionAckCommand command = new ExceptionAckCommand(ByteString.readObject(input));
      command.readFrom(input);
      return command;
   }

   private BackupAckCommand backupAckCommand(ObjectInput input) throws IOException, ClassNotFoundException {
      BackupAckCommand command = new BackupAckCommand(ByteString.readObject(input));
      command.readFrom(input);
      return command;
   }
}

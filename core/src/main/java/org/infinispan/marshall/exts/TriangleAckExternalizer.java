package org.infinispan.marshall.exts;

import static org.infinispan.commons.marshall.Ids.TRIANGLE_ACK_EXTERNALIZER;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.commands.write.PrimaryMultiKeyAckCommand;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.util.concurrent.CommandAckCollector;

/**
 * Externalizer for the triangle acknowledges.
 * <p>
 * It doesn't use the {@link org.infinispan.marshall.DeltaAwareObjectOutput} like the {@link
 * CacheRpcCommandExternalizer}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleAckExternalizer implements AdvancedExternalizer<ReplicableCommand> {

   private final CommandAckCollector commandAckCollector;

   public TriangleAckExternalizer(GlobalComponentRegistry registry) {
      this.commandAckCollector = registry.getCommandAckCollector();
   }

   public Set<Class<? extends ReplicableCommand>> getTypeClasses() {
      //noinspection unchecked
      return Util.asSet(BackupAckCommand.class, ExceptionAckCommand.class,
            PrimaryMultiKeyAckCommand.class, BackupMultiKeyAckCommand.class);
   }

   public Integer getId() {
      return TRIANGLE_ACK_EXTERNALIZER;
   }

   public void writeObject(ObjectOutput output, ReplicableCommand object) throws IOException {
      output.writeByte(object.getCommandId());
      object.writeTo(output);
   }

   public ReplicableCommand readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      switch (input.readByte()) {
         case PrimaryAckCommand.COMMAND_ID:
            return primaryAckCommand(input);
         case BackupAckCommand.COMMAND_ID:
            return backupAckCommand(input);
         case ExceptionAckCommand.COMMAND_ID:
            return exceptionAckCommand(input);
         case PrimaryMultiKeyAckCommand.COMMAND_ID:
            return primaryMultiKeyAckCommand(input);
         case BackupMultiKeyAckCommand.COMMAND_ID:
            return backupMultiKeyAckCommand(input);
         default:
            throw new IllegalStateException();
      }
   }

   private ReplicableCommand backupMultiKeyAckCommand(ObjectInput input) throws IOException, ClassNotFoundException {
      BackupMultiKeyAckCommand command = new BackupMultiKeyAckCommand(commandAckCollector);
      command.readFrom(input);
      return command;
   }

   private ReplicableCommand primaryMultiKeyAckCommand(ObjectInput input) throws IOException, ClassNotFoundException {
      PrimaryMultiKeyAckCommand command = new PrimaryMultiKeyAckCommand(commandAckCollector);
      command.readFrom(input);
      return command;
   }

   private ReplicableCommand exceptionAckCommand(ObjectInput input) throws IOException, ClassNotFoundException {
      ExceptionAckCommand command = new ExceptionAckCommand(commandAckCollector);
      command.readFrom(input);
      return command;
   }

   private ReplicableCommand backupAckCommand(ObjectInput input) throws IOException, ClassNotFoundException {
      BackupAckCommand command = new BackupAckCommand(commandAckCollector);
      command.readFrom(input);
      return command;
   }

   private ReplicableCommand primaryAckCommand(ObjectInput input) throws IOException, ClassNotFoundException {
      PrimaryAckCommand command = new PrimaryAckCommand(commandAckCollector);
      command.readFrom(input);
      return command;
   }
}

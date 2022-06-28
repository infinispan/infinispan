package org.infinispan.commands.triangle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.ByteString;

/**
 * A single key {@link BackupWriteCommand} for single key non-functional commands.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class SingleKeyBackupWriteCommand extends BackupWriteCommand {

   public static final byte COMMAND_ID = 76;
   private static final Operation[] CACHED_OPERATION = Operation.values();

   private Operation operation;
   private Object key;
   private Object valueOrFunction;
   private Metadata metadata;
   private PrivateMetadata internalMetadata;

   //for testing
   @SuppressWarnings("unused")
   public SingleKeyBackupWriteCommand() {
      super(null);
   }

   public SingleKeyBackupWriteCommand(ByteString cacheName) {
      super(cacheName);
   }

   private static Operation valueOf(int index) {
      return CACHED_OPERATION[index];
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public void setPutKeyValueCommand(PutKeyValueCommand command) {
      this.operation = Operation.WRITE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getValue();
      this.metadata = command.getMetadata();
      this.internalMetadata = command.getInternalMetadata();
   }

   public void setIracPutKeyValueCommand(IracPutKeyValueCommand command) {
      this.operation = Operation.WRITE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getValue();
      this.metadata = command.getMetadata();
      this.internalMetadata = command.getInternalMetadata();
   }

   public void setRemoveCommand(RemoveCommand command, boolean removeExpired) {
      this.operation = removeExpired ? Operation.REMOVE_EXPIRED : Operation.REMOVE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getValue();
      this.internalMetadata = command.getInternalMetadata();
   }

   public void setReplaceCommand(ReplaceCommand command) {
      this.operation = Operation.REPLACE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getNewValue();
      this.metadata = command.getMetadata();
      this.internalMetadata = command.getInternalMetadata();
   }

   public void setComputeCommand(ComputeCommand command) {
      this.operation = command.isComputeIfPresent() ? Operation.COMPUTE_IF_PRESENT : Operation.COMPUTE;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getRemappingBiFunction();
      this.metadata = command.getMetadata();
      this.internalMetadata = command.getInternalMetadata();
   }

   public void setComputeIfAbsentCommand(ComputeIfAbsentCommand command) {
      this.operation = Operation.COMPUTE_IF_ABSENT;
      setCommonAttributesFromCommand(command);
      this.key = command.getKey();
      this.valueOrFunction = command.getMappingFunction();
      this.metadata = command.getMetadata();
      this.internalMetadata = command.getInternalMetadata();
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      writeBase(output);
      MarshallUtil.marshallEnum(operation, output);
      output.writeObject(key);
      output.writeObject(internalMetadata);
      switch (operation) {
         case COMPUTE_IF_PRESENT:
         case COMPUTE_IF_ABSENT:
         case COMPUTE:
         case REPLACE:
         case WRITE:
            output.writeObject(metadata);
         // falls through
         case REMOVE_EXPIRED:
            output.writeObject(valueOrFunction);
            break;
         case REMOVE:
            break;
         default:
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      readBase(input);
      operation = MarshallUtil.unmarshallEnum(input, SingleKeyBackupWriteCommand::valueOf);
      key = input.readObject();
      internalMetadata = (PrivateMetadata) input.readObject();
      switch (operation) {
         case COMPUTE_IF_PRESENT:
         case COMPUTE_IF_ABSENT:
         case COMPUTE:
         case REPLACE:
         case WRITE:
            metadata = (Metadata) input.readObject();
         // falls through
         case REMOVE_EXPIRED:
            valueOrFunction = input.readObject();
            break;
         case REMOVE:
            break;
         default:
      }
   }

   @Override
   public String toString() {
      return "SingleKeyBackupWriteCommand{" + toStringFields() + '}';
   }

   @Override
   WriteCommand createWriteCommand() {
      DataWriteCommand command;
      switch (operation) {
         case REMOVE:
            command = new RemoveCommand(key, null, false, segmentId, getFlags(), getCommandInvocationId());
            break;
         case WRITE:
            command = EnumUtil.containsAny(getFlags(), FlagBitSets.IRAC_UPDATE) ?
                  new IracPutKeyValueCommand(key, segmentId, getCommandInvocationId(), valueOrFunction, metadata, internalMetadata) :
                  new PutKeyValueCommand(key, valueOrFunction, false, false, metadata, segmentId, getFlags(), getCommandInvocationId());
            break;
         case COMPUTE:
            command = new ComputeCommand(key, (BiFunction) valueOrFunction, false, segmentId, getFlags(),
                  getCommandInvocationId(), metadata);
            break;
         case REPLACE:
            command = new ReplaceCommand(key, null, valueOrFunction, false, metadata, segmentId, getFlags(),
                  getCommandInvocationId());
            break;
         case REMOVE_EXPIRED:
            // Doesn't matter if it is max idle or not - important thing is that it raises expired event
            command = new RemoveExpiredCommand(key, valueOrFunction, null, false, segmentId, getFlags(),
                  getCommandInvocationId());
            break;
         case COMPUTE_IF_PRESENT:
            command = new ComputeCommand(key, (BiFunction) valueOrFunction, true, segmentId, getFlags(),
                  getCommandInvocationId(), metadata);
            break;
         case COMPUTE_IF_ABSENT:
            command = new ComputeIfAbsentCommand(key, (Function) valueOrFunction, segmentId, getFlags(),
                  getCommandInvocationId(), metadata);
            break;
         default:
            throw new IllegalStateException("Unknown operation " + operation);
      }
      command.setInternalMetadata(internalMetadata);
      return command;
   }

   @Override
   String toStringFields() {
      return super.toStringFields() +
            ", operation=" + operation +
            ", key=" + key +
            ", valueOrFunction=" + valueOrFunction +
            ", metadata=" + metadata +
            ", internalMetadata=" + internalMetadata;
   }

   private enum Operation {
      WRITE,
      REMOVE,
      REMOVE_EXPIRED,
      REPLACE,
      COMPUTE,
      COMPUTE_IF_PRESENT,
      COMPUTE_IF_ABSENT
   }
}

package org.infinispan.commands.triangle;

import static org.infinispan.commands.triangle.SingleKeyBackupWriteCommand.Operation.COMPUTE;
import static org.infinispan.commands.triangle.SingleKeyBackupWriteCommand.Operation.COMPUTE_IF_ABSENT;
import static org.infinispan.commands.triangle.SingleKeyBackupWriteCommand.Operation.COMPUTE_IF_PRESENT;
import static org.infinispan.commands.triangle.SingleKeyBackupWriteCommand.Operation.REMOVE;
import static org.infinispan.commands.triangle.SingleKeyBackupWriteCommand.Operation.REMOVE_EXPIRED;
import static org.infinispan.commands.triangle.SingleKeyBackupWriteCommand.Operation.REPLACE;
import static org.infinispan.commands.triangle.SingleKeyBackupWriteCommand.Operation.WRITE;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * A single key {@link BackupWriteCommand} for single key non-functional commands.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.SINGLE_KEY_BACKUP_WRITE_COMMAND)
public class SingleKeyBackupWriteCommand extends BackupWriteCommand {

   public static final byte COMMAND_ID = 76;

   final Operation operation;
   final Object key;
   final Object valueOrFunction;
   final Metadata metadata;
   final PrivateMetadata internalMetadata;

   public static SingleKeyBackupWriteCommand create(ByteString cacheName, PutKeyValueCommand command, long sequence, int segmentId) {
      return new SingleKeyBackupWriteCommand(cacheName, command, sequence, segmentId, WRITE,
            command.getKey(), command.getValue(), command.getMetadata(), command.getInternalMetadata());
   }

   public static SingleKeyBackupWriteCommand create(ByteString cacheName, RemoveCommand command, long sequence, int segmentId) {
      boolean removeExpired = command instanceof RemoveExpiredCommand;
      Operation operation = removeExpired ? REMOVE_EXPIRED : REMOVE;
      Object value = removeExpired ? command.getValue() : null;
      return new SingleKeyBackupWriteCommand(cacheName, command, sequence, segmentId, operation, command.getKey(), value,
            null, command.getInternalMetadata());
   }

   public static SingleKeyBackupWriteCommand create(ByteString cacheName, ReplaceCommand command, long sequence, int segmentId) {
      return new SingleKeyBackupWriteCommand(cacheName, command, sequence, segmentId, REPLACE, command.getKey(),
            command.getNewValue(), command.getMetadata(), command.getInternalMetadata());
   }


   public static SingleKeyBackupWriteCommand create(ByteString cacheName, ComputeIfAbsentCommand command, long sequence, int segmentId) {
      return new SingleKeyBackupWriteCommand(cacheName, command, sequence, segmentId, COMPUTE_IF_ABSENT,
            command.getKey(), command.getMappingFunction(), command.getMetadata(), command.getInternalMetadata());
   }

   public static SingleKeyBackupWriteCommand create(ByteString cacheName, ComputeCommand command, long sequence, int segmentId) {
      Operation operation = command.isComputeIfPresent() ? COMPUTE_IF_PRESENT : COMPUTE;
      return new SingleKeyBackupWriteCommand(cacheName, command, sequence, segmentId, operation, command.getKey(),
            command.getRemappingBiFunction(), command.getMetadata(), command.getInternalMetadata());
   }

   public static SingleKeyBackupWriteCommand create(ByteString cacheName, IracPutKeyValueCommand command, long sequence, int segmentId) {
      return new SingleKeyBackupWriteCommand(cacheName, command, sequence, segmentId, WRITE, command.getKey(),
            command.getValue(), command.getMetadata(), command.getInternalMetadata());
   }

   private SingleKeyBackupWriteCommand(ByteString cacheName, WriteCommand command, long sequence, int segmentId,
                                      Operation operation, Object key, Object valueOrFunction, Metadata metadata,
                                       PrivateMetadata internalMetadata) {
      super(cacheName, command, sequence, segmentId);
      this.operation = operation;
      this.key = key;
      this.valueOrFunction = valueOrFunction;
      this.metadata = metadata;
      this.internalMetadata = internalMetadata;
   }

   @ProtoFactory
   SingleKeyBackupWriteCommand(ByteString cacheName, CommandInvocationId commandInvocationId, int topologyId,
                               long flags, long sequence, int segmentId, Operation operation, MarshallableObject<?> key,
                               MarshallableObject<?> valueOrFunction, MarshallableObject<Metadata> metadata,
                               PrivateMetadata internalMetadata) {
      super(cacheName, commandInvocationId, topologyId, flags, sequence, segmentId);
      this.operation = operation;
      this.key = MarshallableObject.unwrap(key);
      this.valueOrFunction = MarshallableObject.unwrap(valueOrFunction);
      this.metadata = MarshallableObject.unwrap(metadata);
      this.internalMetadata = internalMetadata;
   }

   @ProtoField(7)
   Operation getOperation() {
      return operation;
   }

   @ProtoField(8)
   MarshallableObject<?> getKey() {
      return MarshallableObject.create(key);
   }

   @ProtoField(9)
   MarshallableObject<?> getValueOrFunction() {
      return MarshallableObject.create(valueOrFunction);
   }

   @ProtoField(10)
   MarshallableObject<Metadata> getMetadata() {
      return MarshallableObject.create(metadata);
   }

   @ProtoField(11)
   PrivateMetadata getInternalMetadata() {
      return internalMetadata;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
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
            command = new ComputeCommand(key, (BiFunction<?, ?, ?>) valueOrFunction, false, segmentId, getFlags(),
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
            command = new ComputeCommand(key, (BiFunction<?, ?, ?>) valueOrFunction, true, segmentId, getFlags(),
                  getCommandInvocationId(), metadata);
            break;
         case COMPUTE_IF_ABSENT:
            command = new ComputeIfAbsentCommand(key, (Function<?, ?>) valueOrFunction, segmentId, getFlags(),
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

   @Proto
   @ProtoName(value = "SingleKeyBackupOperation")
   @ProtoTypeId(ProtoStreamTypeIds.SINGLE_KEY_BACKUP_WRITE_COMMAND_OPERATION)
   public enum Operation {
      WRITE,
      REMOVE,
      REMOVE_EXPIRED,
      REPLACE,
      COMPUTE,
      COMPUTE_IF_PRESENT,
      COMPUTE_IF_ABSENT
   }
}

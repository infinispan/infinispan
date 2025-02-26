package org.infinispan.commands.triangle;

import static org.infinispan.commands.triangle.SingleKeyFunctionalBackupWriteCommand.Operation.READ_WRITE;
import static org.infinispan.commands.triangle.SingleKeyFunctionalBackupWriteCommand.Operation.READ_WRITE_KEY_VALUE;
import static org.infinispan.commands.triangle.SingleKeyFunctionalBackupWriteCommand.Operation.WRITE_ONLY;
import static org.infinispan.commands.triangle.SingleKeyFunctionalBackupWriteCommand.Operation.WRITE_ONLY_KEY_VALUE;
import static org.infinispan.commands.write.ValueMatcher.MATCH_ALWAYS;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.functional.AbstractWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.DataConversion;
import org.infinispan.functional.impl.Params;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * A single key {@link BackupWriteCommand} for single key functional commands.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SINGLE_KEY_FUNCTIONAL_BACKUP_WRITE_COMMAND)
public class SingleKeyFunctionalBackupWriteCommand extends FunctionalBackupWriteCommand {

   private Operation operation;
   private Object key;
   private Object value;
   private Object prevValue;
   private Metadata prevMetadata;

   private SingleKeyFunctionalBackupWriteCommand(ByteString cacheName, AbstractWriteKeyCommand<?, ?> command, long sequence, int segmentId,
                                                 Operation operation, Object key, Object value, Object prevValue,
                                                 Metadata prevMetadata, Object function) {
      super(cacheName, command, sequence, segmentId, function);
      this.operation = operation;
      this.key = key;
      this.value = value;
      this.prevValue = prevValue;
      this.prevMetadata = prevMetadata;
   }

   @ProtoFactory
   SingleKeyFunctionalBackupWriteCommand(ByteString cacheName, CommandInvocationId commandInvocationId, int topologyId,
                                         long flags, long sequence, int segmentId, MarshallableObject<?> function,
                                         Params params, DataConversion keyDataConversion, DataConversion valueDataConversion,
                                         Operation operation, MarshallableObject<?> key, MarshallableObject<?> value,
                                         MarshallableObject<?> prevValue, MarshallableObject<Metadata> prevMetadata) {
      super(cacheName, commandInvocationId, topologyId, flags, sequence, segmentId, function, params, keyDataConversion,
            valueDataConversion);
      this.operation = operation;
      this.key = MarshallableObject.unwrap(key);
      this.value = MarshallableObject.unwrap(value);
      this.prevValue = MarshallableObject.unwrap(prevValue);
      this.prevMetadata = MarshallableObject.unwrap(prevMetadata);
   }

   public static SingleKeyFunctionalBackupWriteCommand create(ByteString cacheName, ReadWriteKeyCommand<?, ?, ?> command, long sequence, int segmentId) {
      return new SingleKeyFunctionalBackupWriteCommand(cacheName, command, sequence, segmentId, READ_WRITE, command.getKey(), null, null,
            null, command.getFunction());
   }

   public static SingleKeyFunctionalBackupWriteCommand create(ByteString cacheName, ReadWriteKeyValueCommand<?, ?, ?, ?> command, long sequence, int segmentId) {
      return new SingleKeyFunctionalBackupWriteCommand(cacheName, command, sequence, segmentId, READ_WRITE_KEY_VALUE, command.getKey(), command.getArgument(),
            command.getPrevValue(), command.getPrevMetadata(), command.getBiFunction());
   }


   public static SingleKeyFunctionalBackupWriteCommand create(ByteString cacheName, WriteOnlyKeyValueCommand<?, ?, ?> command, long sequence, int segmentId) {
      return new SingleKeyFunctionalBackupWriteCommand(cacheName, command, sequence, segmentId, WRITE_ONLY_KEY_VALUE, command.getKey(), command.getArgument(),
            null, null, command.getBiConsumer());
   }

   public static SingleKeyFunctionalBackupWriteCommand create(ByteString cacheName, WriteOnlyKeyCommand<?, ?> command, long sequence, int segmentId) {
      return new SingleKeyFunctionalBackupWriteCommand(cacheName, command, sequence, segmentId, WRITE_ONLY, command.getKey(), null, null,
            null, command.getConsumer());
   }

   @ProtoField(11)
   Operation getOperation() {
      return operation;
   }

   @ProtoField(12)
   MarshallableObject<?> getKey() {
      return MarshallableObject.create(key);
   }

   @ProtoField(13)
   MarshallableObject<?> getValue() {
      return MarshallableObject.create(value);
   }

   @ProtoField(14)
   MarshallableObject<?> getPrevValue() {
      return MarshallableObject.create(prevValue);
   }

   @ProtoField(15)
   MarshallableObject<Metadata> getPrevMetadata() {
      return MarshallableObject.create(prevMetadata);
   }

   @Override
   WriteCommand createWriteCommand() {
      switch (operation) {
         case READ_WRITE:
            //noinspection unchecked
            return new ReadWriteKeyCommand(key, (Function) function, segmentId, getCommandInvocationId(), MATCH_ALWAYS,
                  params, keyDataConversion, valueDataConversion);
         case READ_WRITE_KEY_VALUE:
            //noinspection unchecked
            ReadWriteKeyValueCommand cmd = new ReadWriteKeyValueCommand(key, value, (BiFunction) function, segmentId,
                  getCommandInvocationId(), MATCH_ALWAYS, params, keyDataConversion, valueDataConversion);
            cmd.setPrevValueAndMetadata(prevValue, prevMetadata);
            return cmd;
         case WRITE_ONLY:
            //noinspection unchecked
            return new WriteOnlyKeyCommand(key, (Consumer) function, segmentId, getCommandInvocationId(), MATCH_ALWAYS,
                  params, keyDataConversion, valueDataConversion);
         case WRITE_ONLY_KEY_VALUE:
            //noinspection unchecked
            return new WriteOnlyKeyValueCommand(key, value, (BiConsumer) function, segmentId, getCommandInvocationId(),
                  MATCH_ALWAYS, params, keyDataConversion, valueDataConversion);
         default:
            throw new IllegalStateException("Unknown operation " + operation);
      }
   }

   @Proto
   @ProtoName(value = "SingleKeyFunctionBackupOperation")
   @ProtoTypeId(ProtoStreamTypeIds.SINGLE_KEY_FUNCTIONAL_BACKUP_WRITE_COMMAND_OPERATION)
   public enum Operation {
      READ_WRITE_KEY_VALUE,
      READ_WRITE,
      WRITE_ONLY_KEY_VALUE,
      WRITE_ONLY
   }
}

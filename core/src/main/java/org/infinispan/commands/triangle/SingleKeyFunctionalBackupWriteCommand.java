package org.infinispan.commands.triangle;

import static org.infinispan.commands.write.ValueMatcher.MATCH_ALWAYS;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.commands.functional.AbstractWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.ByteString;

/**
 * A single key {@link BackupWriteCommand} for single key functional commands.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class SingleKeyFunctionalBackupWriteCommand extends FunctionalBackupWriteCommand {

   public static final byte COMMAND_ID = 77;
   private static final Operation[] CACHED_OPERATION = Operation.values();

   private Operation operation;
   private Object key;
   private Object value;
   private Object prevValue;
   private Metadata prevMetadata;

   //for testing
   @SuppressWarnings("unused")
   public SingleKeyFunctionalBackupWriteCommand() {
      super(null);
   }

   public SingleKeyFunctionalBackupWriteCommand(ByteString cacheName) {
      super(cacheName);
   }

   private static Operation valueOf(int index) {
      return CACHED_OPERATION[index];
   }

   public void init(InvocationContextFactory factory, AsyncInterceptorChain chain,
                    ComponentRegistry componentRegistry) {
      injectDependencies(factory, chain);
      this.componentRegistry = componentRegistry;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public void setReadWriteKeyCommand(ReadWriteKeyCommand command) {
      this.operation = Operation.READ_WRITE;
      setCommonFields(command);
      this.function = command.getFunction();
   }

   public void setReadWriteKeyValueCommand(ReadWriteKeyValueCommand command) {
      this.operation = Operation.READ_WRITE_KEY_VALUE;
      setCommonFields(command);
      this.function = command.getBiFunction();
      this.value = command.getArgument();
      this.prevValue = command.getPrevValue();
      this.prevMetadata = command.getPrevMetadata();
   }

   public void setWriteOnlyKeyValueCommand(WriteOnlyKeyValueCommand command) {
      this.operation = Operation.WRITE_ONLY_KEY_VALUE;
      setCommonFields(command);
      this.function = command.getBiConsumer();
      this.value = command.getArgument();
   }

   public void setWriteOnlyKeyCommand(WriteOnlyKeyCommand command) {
      this.operation = Operation.WRITE_ONLY;
      setCommonFields(command);
      this.function = command.getConsumer();
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      writeBase(output);
      writeFunctionAndParams(output);
      MarshallUtil.marshallEnum(operation, output);
      switch (operation) {
         case READ_WRITE_KEY_VALUE:
            output.writeEntry(key, prevValue, prevMetadata);
            output.writeValue(value);
            break;
         case WRITE_ONLY_KEY_VALUE:
            output.writeKeyValue(key, value);
            break;
         case READ_WRITE:
         case WRITE_ONLY:
         default:
            output.writeKey(key);
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      readBase(input);
      readFunctionAndParams(input);
      operation = MarshallUtil.unmarshallEnum(input, SingleKeyFunctionalBackupWriteCommand::valueOf);
      MarshalledEntryImpl me = MarshalledEntryUtil.read(input);
      key = me.getKey();
      switch (operation) {
         case READ_WRITE_KEY_VALUE:
            prevValue = me.getValue();
            prevMetadata = me.metadata();
            value = MarshalledEntryUtil.readValue(input);
            break;
         case WRITE_ONLY_KEY_VALUE:
            value = me.getValue();
            break;
         case READ_WRITE:
         case WRITE_ONLY:
         default:
      }
   }

   @Override
   WriteCommand createWriteCommand() {
      switch (operation) {
         case READ_WRITE:
            //noinspection unchecked
            return new ReadWriteKeyCommand(key, (Function) function, segmentId, getCommandInvocationId(), MATCH_ALWAYS,
                  params, keyDataConversion, valueDataConversion, componentRegistry);
         case READ_WRITE_KEY_VALUE:
            return createReadWriteKeyValueCommand();
         case WRITE_ONLY:
            //noinspection unchecked
            return new WriteOnlyKeyCommand(key, (Consumer) function, segmentId, getCommandInvocationId(), MATCH_ALWAYS,
                  params, keyDataConversion, valueDataConversion, componentRegistry);
         case WRITE_ONLY_KEY_VALUE:
            //noinspection unchecked
            return new WriteOnlyKeyValueCommand(key, value, (BiConsumer) function, segmentId, getCommandInvocationId(),
                  MATCH_ALWAYS, params, keyDataConversion, valueDataConversion, componentRegistry);
         default:
            throw new IllegalStateException("Unknown operation " + operation);
      }
   }

   private <C extends AbstractWriteKeyCommand> void setCommonFields(C command) {
      setCommonAttributesFromCommand(command);
      setFunctionalCommand(command);
      this.key = command.getKey();
   }

   private ReadWriteKeyValueCommand createReadWriteKeyValueCommand() {
      //noinspection unchecked
      ReadWriteKeyValueCommand cmd = new ReadWriteKeyValueCommand(key, value, (BiFunction) function, segmentId,
            getCommandInvocationId(), MATCH_ALWAYS, params, keyDataConversion, valueDataConversion, componentRegistry);
      cmd.setPrevValueAndMetadata(prevValue, prevMetadata);
      return cmd;
   }

   private enum Operation {
      READ_WRITE_KEY_VALUE,
      READ_WRITE,
      WRITE_ONLY_KEY_VALUE,
      WRITE_ONLY
   }
}

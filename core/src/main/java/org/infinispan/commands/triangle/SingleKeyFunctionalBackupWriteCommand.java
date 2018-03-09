package org.infinispan.commands.triangle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
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
   private CommandInvocationId lastInvocationId;

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
                    ComponentRegistry componentRegistry, InvocationManager invocationManager) {
      injectDependencies(factory, chain, invocationManager);
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
   public void writeTo(ObjectOutput output) throws IOException {
      writeBase(output);
      writeFunctionAndParams(output);
      MarshallUtil.marshallEnum(operation, output);
      output.writeObject(key);
      CommandInvocationId.writeTo(output, lastInvocationId);
      switch (operation) {
         case READ_WRITE_KEY_VALUE:
         case WRITE_ONLY_KEY_VALUE:
            output.writeObject(value);
         case READ_WRITE:
         case WRITE_ONLY:
         default:
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      readBase(input);
      readFunctionAndParams(input);
      operation = MarshallUtil.unmarshallEnum(input, SingleKeyFunctionalBackupWriteCommand::valueOf);
      key = input.readObject();
      lastInvocationId = CommandInvocationId.readFrom(input);
      switch (operation) {
         case READ_WRITE_KEY_VALUE:
         case WRITE_ONLY_KEY_VALUE:
            value = input.readObject();
         case READ_WRITE:
         case WRITE_ONLY:
         default:
      }
   }

   @Override
   WriteCommand createWriteCommand() {
      WriteCommand command;
      switch (operation) {
         case READ_WRITE:
            //noinspection unchecked
            command = new ReadWriteKeyCommand(key, (Function) function, getCommandInvocationId(), params, keyDataConversion, valueDataConversion, invocationManager, componentRegistry);
            break;
         case READ_WRITE_KEY_VALUE:
            //noinspection unchecked
            command = new ReadWriteKeyValueCommand(key, value, (BiFunction) function,
                  getCommandInvocationId(),  params, keyDataConversion, valueDataConversion, invocationManager, componentRegistry);
            break;
         case WRITE_ONLY:
            //noinspection unchecked
            command = new WriteOnlyKeyCommand(key, (Consumer) function, getCommandInvocationId(), params, keyDataConversion, valueDataConversion, invocationManager, componentRegistry);
            break;
         case WRITE_ONLY_KEY_VALUE:
            //noinspection unchecked
            command = new WriteOnlyKeyValueCommand(key, value, (BiConsumer) function, getCommandInvocationId(),
                  params, keyDataConversion, valueDataConversion, invocationManager, componentRegistry);
            break;
         default:
            throw new IllegalStateException("Unknown operation " + operation);
      }
      command.setLastInvocationId(key, lastInvocationId);
      return command;
   }

   private <C extends AbstractWriteKeyCommand> void setCommonFields(C command) {
      setCommonAttributesFromCommand(command);
      setFunctionalCommand(command);
      this.key = command.getKey();
      this.lastInvocationId = command.getLastInvocationId(command.getKey());
   }

   private enum Operation {
      READ_WRITE_KEY_VALUE,
      READ_WRITE,
      WRITE_ONLY_KEY_VALUE,
      WRITE_ONLY
   }
}

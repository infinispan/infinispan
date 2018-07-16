package org.infinispan.commands.triangle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.commands.functional.AbstractWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.util.ByteString;

/**
 * A multi-key {@link BackupWriteCommand} for {@link WriteOnlyManyCommand} and {@link ReadWriteManyCommand}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class MultiKeyFunctionalBackupWriteCommand extends FunctionalBackupWriteCommand {

   public static final byte COMMAND_ID = 80;

   private boolean writeOnly;
   private Collection<?> keys;

   //for testing
   @SuppressWarnings("unused")
   public MultiKeyFunctionalBackupWriteCommand() {
      super(null);
   }

   public MultiKeyFunctionalBackupWriteCommand(ByteString cacheName) {
      super(cacheName);
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

   public <K, V> void setWriteOnly(WriteOnlyManyCommand<K, V> command, Collection<Object> keys) {
      setCommonAttributesFromCommand(command);
      setFunctionalCommand(command);
      this.writeOnly = true;
      this.keys = keys;
      this.function = command.getConsumer();
   }

   public <K, V, R> void setReadWrite(ReadWriteManyCommand<K, V, R> command, Collection<Object> keys) {
      setCommonAttributesFromCommand(command);
      setFunctionalCommand(command);
      this.writeOnly = false;
      this.keys = keys;
      this.function = command.getBiFunction();
   }

   @Override
   public void writeTo(ObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      writeBase(output);
      writeFunctionAndParams(output);
      output.writeBoolean(writeOnly);
      MarshalledEntryUtil.marshallCollection(keys, (key, factory, out) -> MarshalledEntryUtil.writeKey(key));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      readBase(input);
      readFunctionAndParams(input);
      writeOnly = input.readBoolean();
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new, MarshalledEntryUtil::readKey);
   }

   @Override
   WriteCommand createWriteCommand() {
      //noinspection unchecked
      AbstractWriteManyCommand cmd = writeOnly ?
            new WriteOnlyManyCommand(keys, (Consumer) function, params, getCommandInvocationId(),
                  keyDataConversion, valueDataConversion, componentRegistry) :
            new ReadWriteManyCommand(keys, (Function) function, params, getCommandInvocationId(),
                  keyDataConversion, valueDataConversion, componentRegistry);
      cmd.setForwarded(true);
      return cmd;
   }

}

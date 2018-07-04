package org.infinispan.commands.triangle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.infinispan.commands.functional.AbstractWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.util.ByteString;
import org.infinispan.util.TriangleFunctionsUtil;

/**
 * A multi-key {@link BackupWriteCommand} for {@link WriteOnlyManyEntriesCommand} and {@link ReadWriteManyEntriesCommand}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class MultiEntriesFunctionalBackupWriteCommand extends FunctionalBackupWriteCommand {

   public static final byte COMMAND_ID = 79;

   private boolean writeOnly;
   private Map<?, ?> entries;

   //for testing
   @SuppressWarnings("unused")
   public MultiEntriesFunctionalBackupWriteCommand() {
      super(null);
   }

   public MultiEntriesFunctionalBackupWriteCommand(ByteString cacheName) {
      super(cacheName);
   }

   public void init(InvocationContextFactory factory, AsyncInterceptorChain chain,
                    ComponentRegistry componentRegistry) {
      injectDependencies(factory, chain);
      this.componentRegistry = componentRegistry;
   }

   public <K, V, T> void setWriteOnly(WriteOnlyManyEntriesCommand<K, V, T> command, Collection<Object> keys) {
      setCommonAttributesFromCommand(command);
      setFunctionalCommand(command);
      writeOnly = true;
      this.entries = TriangleFunctionsUtil.filterEntries(command.getArguments(), keys);
      this.function = command.getBiConsumer();
   }

   public <K, V, T, R> void setReadWrite(ReadWriteManyEntriesCommand<K, V, T, R> command, Collection<Object> keys) {
      setCommonAttributesFromCommand(command);
      setFunctionalCommand(command);
      writeOnly = false;
      this.entries = TriangleFunctionsUtil.filterEntries(command.getArguments(), keys);
      this.function = command.getBiFunction();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      writeBase(output);
      writeFunctionAndParams(output);
      output.writeBoolean(writeOnly);
      MarshalledEntryUtil.marshallMap(entries, entryFactory, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      readBase(input);
      readFunctionAndParams(input);
      writeOnly = input.readBoolean();
      entries = MarshalledEntryUtil.unmarshallMap(input, HashMap::new);
   }

   @Override
   WriteCommand createWriteCommand() {
      //noinspection unchecked
      AbstractWriteManyCommand cmd = writeOnly ?
            new WriteOnlyManyEntriesCommand(entries, (BiConsumer) function, params, getCommandInvocationId(),
                  keyDataConversion, valueDataConversion, componentRegistry) :
            new ReadWriteManyEntriesCommand(entries, (BiFunction) function, params, getCommandInvocationId(),
                  keyDataConversion, valueDataConversion, componentRegistry);
      cmd.setForwarded(true);
      return cmd;
   }

}

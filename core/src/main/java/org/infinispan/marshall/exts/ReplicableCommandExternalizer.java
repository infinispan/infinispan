package org.infinispan.marshall.exts;

import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.*;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.DeltaAwareObjectOutput;
import org.infinispan.manager.impl.ReplicableCommandRunnable;
import org.infinispan.manager.impl.ReplicableCommandManagerFunction;
import org.infinispan.marshall.core.Ids;
import org.infinispan.topology.CacheTopologyControlCommand;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Set;

/**
 * ReplicableCommandExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ReplicableCommandExternalizer extends AbstractExternalizer<ReplicableCommand> {
   private final RemoteCommandsFactory cmdFactory;
   private final GlobalComponentRegistry globalComponentRegistry;

   public ReplicableCommandExternalizer(RemoteCommandsFactory cmdFactory, GlobalComponentRegistry globalComponentRegistry) {
      this.cmdFactory = cmdFactory;
      this.globalComponentRegistry = globalComponentRegistry;
   }

   @Override
   public void writeObject(ObjectOutput output, ReplicableCommand command) throws IOException {
      writeCommandHeader(output, command);
      writeCommandParameters(output, command);
   }

   protected void writeCommandParameters(ObjectOutput output, ReplicableCommand command) throws IOException {
      DeltaAwareObjectOutput deltaAwareObjectOutput = output instanceof DeltaAwareObjectOutput ?
            (DeltaAwareObjectOutput) output :
            new DeltaAwareObjectOutput(output);
      Object[] args = command.getParameters();
      int numArgs = (args == null ? 0 : args.length);

      UnsignedNumeric.writeUnsignedInt(output, numArgs);
      for (int i = 0; i < numArgs; i++) {
         deltaAwareObjectOutput.writeObject(args[i]);
      }
      command.writeTo(deltaAwareObjectOutput);
      if (command instanceof TopologyAffectedCommand) {
         output.writeInt(((TopologyAffectedCommand) command).getTopologyId());
      }
   }

   protected void writeCommandHeader(ObjectOutput output, ReplicableCommand command) throws IOException {
      // To decide whether it's a core or user defined command, load them all and check
      Collection<Class<? extends ReplicableCommand>> moduleCommands = getModuleCommands();
      // Write an indexer to separate commands defined external to the
      // infinispan core module from the ones defined via module commands
      if (moduleCommands != null && moduleCommands.contains(command.getClass()))
         output.writeByte(1);
      else
         output.writeByte(0);

      output.writeShort(command.getCommandId());
   }

   @Override
   public ReplicableCommand readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      ReplicableCommand replicableCommand = readCommandHeader(input);
      readCommandParameters(input, replicableCommand);
      return replicableCommand;
   }

   protected ReplicableCommand readCommandHeader(ObjectInput input) throws IOException, ClassNotFoundException {
      byte type = input.readByte();
      short methodId = input.readShort();
      return cmdFactory.fromStream((byte) methodId, readLegacyParameters(input), type);
   }

   protected Object[] readLegacyParameters(ObjectInput input) throws IOException, ClassNotFoundException {
      int numArgs = UnsignedNumeric.readUnsignedInt(input);
      Object[] args = null;
      if (numArgs > 0) {
         args = new Object[numArgs];
         // For DeltaAware instances, nothing special to be done here.
         // Do not merge here since the cache contents are required.
         // Instead, merge in PutKeyValueCommand.perform
         for (int i = 0; i < numArgs; i++) args[i] = input.readObject();
      }
      return args;
   }

   void readCommandParameters(ObjectInput input, ReplicableCommand command) throws IOException, ClassNotFoundException {
      command.readFrom(input);
      if (command instanceof TopologyAffectedCommand) {
         ((TopologyAffectedCommand) command).setTopologyId(input.readInt());
      }
   }

   protected CacheRpcCommand fromStream(byte id, Object[] parameters, byte type, String cacheName) {
      return cmdFactory.fromStream(id, parameters, type, cacheName);
   }

   @Override
   public Integer getId() {
      return Ids.REPLICABLE_COMMAND;
   }

   @Override
   public Set<Class<? extends ReplicableCommand>> getTypeClasses() {
      //noinspection unchecked
      Set<Class<? extends ReplicableCommand>> coreCommands = Util.asSet(
            CacheTopologyControlCommand.class, DistributedExecuteCommand.class, GetKeyValueCommand.class,
            ClearCommand.class, EvictCommand.class, ApplyDeltaCommand.class,
            InvalidateCommand.class, InvalidateL1Command.class,
            PutKeyValueCommand.class,
            PutMapCommand.class, RemoveCommand.class, RemoveExpiredCommand.class,
            ReplaceCommand.class, GetKeysInGroupCommand.class,
            ReadWriteKeyCommand.class, ReadWriteKeyValueCommand.class,
            WriteOnlyKeyCommand.class, WriteOnlyKeyValueCommand.class,
            WriteOnlyManyCommand.class, WriteOnlyManyEntriesCommand.class,
            ReadWriteManyCommand.class, ReadWriteManyEntriesCommand.class, ReplicableCommandRunnable.class,
            ReplicableCommandManagerFunction.class);
      // Search only those commands that replicable and not cache specific replicable commands
      Collection<Class<? extends ReplicableCommand>> moduleCommands = globalComponentRegistry.getModuleProperties().moduleOnlyReplicableCommands();
      if (moduleCommands != null && !moduleCommands.isEmpty()) coreCommands.addAll(moduleCommands);
      return coreCommands;
   }

   private Collection<Class<? extends ReplicableCommand>> getModuleCommands() {
      return globalComponentRegistry.getModuleProperties().moduleCommands();
   }

}

package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Set;

import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.TxReadOnlyKeyCommand;
import org.infinispan.commands.functional.TxReadOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.impl.ReplicableCommandManagerFunction;
import org.infinispan.manager.impl.ReplicableCommandRunnable;
import org.infinispan.marshall.core.Ids;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.topology.HeartBeatCommand;
import org.infinispan.util.ByteString;

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
      // TODO hook in UserAwareObjectOutput Impl
      command.writeTo(output);
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

   private ReplicableCommand readCommandHeader(ObjectInput input) throws IOException {
      byte type = input.readByte();
      short methodId = input.readShort();
      return cmdFactory.fromStream((byte) methodId, type);
   }

   void readCommandParameters(ObjectInput input, ReplicableCommand command) throws IOException, ClassNotFoundException {
      command.readFrom(input);
      if (command instanceof TopologyAffectedCommand) {
         ((TopologyAffectedCommand) command).setTopologyId(input.readInt());
      }
   }

   protected CacheRpcCommand fromStream(byte id, byte type, ByteString cacheName) {
      return cmdFactory.fromStream(id, type, cacheName);
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
            ClearCommand.class, EvictCommand.class,
            InvalidateCommand.class, InvalidateL1Command.class,
            PutKeyValueCommand.class,
            PutMapCommand.class, RemoveCommand.class, RemoveExpiredCommand.class,
            ReplaceCommand.class,
            ComputeCommand.class, ComputeIfAbsentCommand.class,
            GetKeysInGroupCommand.class,
            ReadOnlyKeyCommand.class, ReadOnlyManyCommand.class,
            ReadWriteKeyCommand.class, ReadWriteKeyValueCommand.class,
            WriteOnlyKeyCommand.class, WriteOnlyKeyValueCommand.class,
            WriteOnlyManyCommand.class, WriteOnlyManyEntriesCommand.class,
            ReadWriteManyCommand.class, ReadWriteManyEntriesCommand.class,
            TxReadOnlyKeyCommand.class, TxReadOnlyManyCommand.class,
            ReplicableCommandRunnable.class, ReplicableCommandManagerFunction.class,
            HeartBeatCommand.class);
      // Search only those commands that replicable and not cache specific replicable commands
      Collection<Class<? extends ReplicableCommand>> moduleCommands = globalComponentRegistry.getModuleProperties().moduleOnlyReplicableCommands();
      if (moduleCommands != null && !moduleCommands.isEmpty()) coreCommands.addAll(moduleCommands);
      return coreCommands;
   }

   private Collection<Class<? extends ReplicableCommand>> getModuleCommands() {
      return globalComponentRegistry.getModuleProperties().moduleCommands();
   }

}

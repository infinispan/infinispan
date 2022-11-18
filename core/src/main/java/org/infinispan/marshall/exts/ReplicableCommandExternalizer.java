package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Set;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseTopologyRpcCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.topology.CacheAvailabilityUpdateCommand;
import org.infinispan.commands.topology.CacheJoinCommand;
import org.infinispan.commands.topology.CacheLeaveCommand;
import org.infinispan.commands.topology.CacheShutdownCommand;
import org.infinispan.commands.topology.CacheShutdownRequestCommand;
import org.infinispan.commands.topology.CacheStatusRequestCommand;
import org.infinispan.commands.topology.RebalancePhaseConfirmCommand;
import org.infinispan.commands.topology.RebalancePolicyUpdateCommand;
import org.infinispan.commands.topology.RebalanceStartCommand;
import org.infinispan.commands.topology.RebalanceStatusRequestCommand;
import org.infinispan.commands.topology.TopologyUpdateCommand;
import org.infinispan.commands.topology.TopologyUpdateStableCommand;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.impl.ReplicableManagerFunctionCommand;
import org.infinispan.manager.impl.ReplicableRunnableCommand;
import org.infinispan.marshall.core.Ids;
import org.infinispan.topology.HeartBeatCommand;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.commands.XSiteViewNotificationCommand;

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
      command.writeTo(output);
      if (command instanceof AbstractTopologyAffectedCommand) {
         output.writeInt(((AbstractTopologyAffectedCommand) command).getTopologyId());
      } else if (command instanceof BaseTopologyRpcCommand) {
         output.writeInt(((BaseTopologyRpcCommand) command).getTopologyId());
      } else if (command instanceof TopologyAffectedCommand) {
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
      // To prevent type pollution
      if (command instanceof AbstractTopologyAffectedCommand) {
         ((AbstractTopologyAffectedCommand) command).setTopologyId(input.readInt());
      } else if (command instanceof BaseTopologyRpcCommand) {
         ((BaseTopologyRpcCommand) command).setTopologyId(input.readInt());
      } else if (command instanceof TopologyAffectedCommand) {
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
      Set<Class<? extends ReplicableCommand>> coreCommands = Util.asSet(
            ReplicableRunnableCommand.class, ReplicableManagerFunctionCommand.class,
            HeartBeatCommand.class, CacheStatusRequestCommand.class, RebalancePhaseConfirmCommand.class,
            TopologyUpdateCommand.class, RebalancePolicyUpdateCommand.class,
            RebalanceStartCommand.class, RebalanceStatusRequestCommand.class,
            CacheShutdownCommand.class, CacheShutdownRequestCommand.class, TopologyUpdateStableCommand.class,
            CacheJoinCommand.class, CacheLeaveCommand.class, CacheAvailabilityUpdateCommand.class,
            XSiteViewNotificationCommand.class);
      // Search only those commands that replicable and not cache specific replicable commands
      Collection<Class<? extends ReplicableCommand>> moduleCommands = globalComponentRegistry.getModuleProperties().moduleOnlyReplicableCommands();
      if (!moduleCommands.isEmpty()) coreCommands.addAll(moduleCommands);
      return coreCommands;
   }

   private Collection<Class<? extends ReplicableCommand>> getModuleCommands() {
      return globalComponentRegistry.getModuleProperties().moduleCommands();
   }

}

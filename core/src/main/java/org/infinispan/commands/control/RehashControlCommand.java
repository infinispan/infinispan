package org.infinispan.commands.control;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.RemoteTransactionLogDetails;
import org.infinispan.distribution.TransactionLogger;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.NodeTopologyInfo;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A control command to coordinate rehashes that may occur when nodes join or leave a cluster, when DIST is used as a
 * cache mode.  This complex command coordinates the various phases of a rehash event when a joiner joins or a leaver
 * leaves a cluster running in "distribution" mode.
 * <p />
 * It may break up into several commands in future.
 * <p />
 * In its current form, it is documented on <a href="http://community.jboss.org/wiki/DesignOfDynamicRehashing">this wiki page</a>.
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@Marshallable(externalizer = ReplicableCommandExternalizer.class, id = Ids.REHASH_CONTROL_COMMAND)
public class RehashControlCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 17;

   /* For a detailed description of the interactions involved here, please visit http://community.jboss.org/wiki/DesignOfDynamicRehashing */
   public enum Type {
      LEAVE_DRAIN_TX,
      LEAVE_DRAIN_TX_PREPARES,
      JOIN_TX_LOG_REQ,
      JOIN_TX_FINAL_LOG_REQ,
      JOIN_TX_LOG_CLOSE,
      APPLY_STATE // receive a map of keys and add them to the data container
   }

   Type type;
   Address sender;
   Map<Object, InternalCacheValue> state;
   ConsistentHash oldCH;
   List<Address> nodesLeft;
   ConsistentHash newCH;

   // cache components
   DistributionManager distributionManager;
   Transport transport;
   Configuration configuration;
   DataContainer dataContainer;
   List<WriteCommand> txLogCommands;
   List<PrepareCommand> pendingPrepares;
   CommandsFactory commandsFactory;
   NodeTopologyInfo nodeTopologyInfo;
   private static final Log log = LogFactory.getLog(RehashControlCommand.class);

   public RehashControlCommand() {
   }


   public RehashControlCommand(String cacheName, Type type, Address sender, Map<Object, InternalCacheValue> state,ConsistentHash oldConsistentHash,
                                ConsistentHash consistentHash, List<Address> leavers, CommandsFactory commandsFactory) {
      super(cacheName);
      this.type = type;
      this.sender = sender;
      this.state = state;
      this.oldCH = oldConsistentHash;
      this.newCH = consistentHash;
      this.nodesLeft = leavers;
      this.commandsFactory = commandsFactory;
   }

   public RehashControlCommand(String cacheName, Type type, Address sender, List<WriteCommand> txLogCommands,
                               List<PrepareCommand> pendingPrepares, CommandsFactory commandsFactory) {
      super(cacheName);
      this.type = type;
      this.sender = sender;
      this.txLogCommands = txLogCommands;
      this.pendingPrepares = pendingPrepares;
      this.commandsFactory = commandsFactory;
   }

   public RehashControlCommand(Transport transport) {
      this.transport = transport;
   }

   public void init(DistributionManager distributionManager, Configuration configuration, DataContainer dataContainer,
                    CommandsFactory commandsFactory) {
      this.distributionManager = distributionManager;
      this.configuration = configuration;
      this.dataContainer = dataContainer;
      this.commandsFactory = commandsFactory;

      // we need to "fix" these command lists - essentially propagate the init.  TODO think of a nicer way to do this!!
      for (List<? extends ReplicableCommand> commandList : Arrays.asList(txLogCommands, pendingPrepares)) {
         if (commandList != null) {
            for (ReplicableCommand cmd : commandList) commandsFactory.initializeReplicableCommand(cmd, false);
         }
      }
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      switch (type) {
         case LEAVE_DRAIN_TX: // used for a LEAVE ONLY!!
            distributionManager.applyRemoteTxLog(txLogCommands);
            return null;
         case LEAVE_DRAIN_TX_PREPARES:
            for (PrepareCommand pc : pendingPrepares) pc.perform(null);
            return null;
         case JOIN_TX_LOG_REQ:
            return drainTxLog();
         case JOIN_TX_FINAL_LOG_REQ:
            return lockAndDrainTxLog();
         case JOIN_TX_LOG_CLOSE:
            unlockAndCloseTxLog();
            return null;
         case APPLY_STATE:
            // todo: is this the correct transaction logger ?
            // todo: last argument forLeave is always set to true, is this correct ?
            distributionManager.applyState(newCH, state, distributionManager.getTransactionLogger(), true,sender);
            return null;
      }
      throw new CacheException("Unknown rehash control command type " + type);
   }

   private RemoteTransactionLogDetails drainTxLog() {
      TransactionLogger tl = distributionManager.getTransactionLogger();
      List<WriteCommand> mods = tl.drain();
      return new RemoteTransactionLogDetails(tl.shouldDrainWithoutLock(), mods, null);
   }

   private RemoteTransactionLogDetails lockAndDrainTxLog() {
      TransactionLogger tl = distributionManager.getTransactionLogger();
      return new RemoteTransactionLogDetails(false, tl.drainAndLock(sender), tl.getPendingPrepares());

   }

   private void unlockAndCloseTxLog() {
      TransactionLogger tl = distributionManager.getTransactionLogger();
      tl.unlockAndDisable(sender);
   }



   public Type getType() {
      return type;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{cacheName, (byte) type.ordinal(), sender, state, oldCH, nodesLeft, newCH, txLogCommands, pendingPrepares, nodeTopologyInfo};
   }

   public void setNodeTopologyInfo(NodeTopologyInfo nodeTopologyInfo) {
      this.nodeTopologyInfo = nodeTopologyInfo;
   }

   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      cacheName = (String) parameters[i++];
      type = Type.values()[(Byte) parameters[i++]];
      sender = (Address) parameters[i++];
      state = (Map<Object, InternalCacheValue>) parameters[i++];
      oldCH = (ConsistentHash) parameters[i++];
      nodesLeft = (List<Address>) parameters[i++];
      newCH = (ConsistentHash) parameters[i++];
      txLogCommands = (List<WriteCommand>) parameters[i++];
      pendingPrepares = (List<PrepareCommand>) parameters[i++];
      nodeTopologyInfo = (NodeTopologyInfo) parameters[i++];
   }

   @Override
   public String toString() {
      return "RehashControlCommand{" +
            "type=" + type +
            ", sender=" + sender +
            ", state=" + (state == null ? "N/A" : state.size()) +
            ", oldConsistentHash=" + oldCH +
            ", nodesLeft=" + nodesLeft +
            ", consistentHash=" + newCH +
            ", txLogCommands=" + (txLogCommands == null ? "N/A" : txLogCommands.size()) +
            ", pendingPrepares=" + (pendingPrepares == null ? "N/A" : pendingPrepares.size()) +
            ", nodeTopologyInfo=" + nodeTopologyInfo +
            '}';
   }
}

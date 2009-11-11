package org.infinispan.commands.control;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ConsistentHash;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

/**
 * A control command to coordinate rehashes that may occur when nodes join or leave a cluster, when DIST is used as a
 * cache mode.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = ReplicableCommandExternalizer.class, id = Ids.REHASH_CONTROL_COMMAND)
public class RehashControlCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 17;

   public enum Type {
      JOIN_REQ, JOIN_REHASH_START, JOIN_REHASH_END, JOIN_COMPLETE, JOIN_ABORT, PULL_STATE, PUSH_STATE, DRAIN_TX, DRAIN_TX_PREPARES
   }

   Type type;
   Address sender;
   Map<Object, InternalCacheValue> state;
   ConsistentHash consistentHash;

   // cache components
   DistributionManager distributionManager;
   Transport transport;
   Configuration configuration;
   DataContainer dataContainer;
   List<WriteCommand> txLogCommands;
   List<PrepareCommand> pendingPrepares;
   CommandsFactory commandsFactory;

   public RehashControlCommand() {
   }


   public RehashControlCommand(String cacheName, Type type, Address sender, Map<Object, InternalCacheValue> state,
                               ConsistentHash consistentHash, CommandsFactory commandsFactory) {
      super(cacheName);
      this.type = type;
      this.sender = sender;
      this.state = state;
      this.consistentHash = consistentHash;
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
            for (ReplicableCommand cmd : commandList) commandsFactory.initializeReplicableCommand(cmd);
         }
      }
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      switch (type) {
         case JOIN_REQ:
            return distributionManager.requestPermissionToJoin(sender);
         case JOIN_REHASH_START:
            distributionManager.informRehashOnJoin(sender, true);
            return null;
         case JOIN_REHASH_END:
            distributionManager.informRehashOnJoin(sender, false);
            return null;
         case JOIN_COMPLETE:
            distributionManager.notifyJoinComplete(sender);
            return null;
         case PULL_STATE:
            return pullState();
         case PUSH_STATE:
            return pushState();
         case DRAIN_TX:
            distributionManager.applyRemoteTxLog(txLogCommands);
            return null;
         case DRAIN_TX_PREPARES:
            for (PrepareCommand pc : pendingPrepares) pc.perform(null);
            return null;
      }
      throw new CacheException("Unknown rehash control command type " + type);
   }

   public Map<Object, InternalCacheValue> pullState() throws CacheLoaderException {
      Address self = transport.getAddress();
      ConsistentHash oldCH = distributionManager.getConsistentHash();
      int numCopies = configuration.getNumOwners();

      Map<Object, InternalCacheValue> state = new HashMap<Object, InternalCacheValue>();
      for (InternalCacheEntry ice : dataContainer) {
         Object k = ice.getKey();
         if (shouldAddToMap(k, oldCH, numCopies, self)) state.put(k, ice.toInternalCacheValue());
      }

      CacheStore cacheStore = distributionManager.getCacheStoreForRehashing();
      if (cacheStore != null) {
         for (InternalCacheEntry ice : cacheStore.loadAll()) {
            Object k = ice.getKey();
            if (!state.containsKey(k) && shouldAddToMap(k, oldCH, numCopies, self))
               state.put(k, ice.toInternalCacheValue());
         }
      }
      return state;
   }

   final boolean shouldAddToMap(Object k, ConsistentHash oldCH, int numCopies, Address self) {
      // if the current address is the current "owner" of this key (in old_ch), and the requestor is in the owner list
      // in new_ch, then add this to the map.
      List<Address> oldOwnerList = oldCH.locate(k, numCopies);
      if (oldOwnerList.size() > 0 && self.equals(oldOwnerList.get(0))) {
         List<Address> newOwnerList = consistentHash.locate(k, numCopies);
         if (newOwnerList.contains(sender)) return true;
      }
      return false;
   }

   public Object pushState() {
      distributionManager.applyReceivedState(state);
      return null;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{cacheName, (byte) type.ordinal(), sender, state, consistentHash, txLogCommands, pendingPrepares};
   }

   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      cacheName = (String) parameters[i++];
      type = Type.values()[(Byte) parameters[i++]];
      sender = (Address) parameters[i++];
      state = (Map<Object, InternalCacheValue>) parameters[i++];
      consistentHash = (ConsistentHash) parameters[i++];
      txLogCommands = (List<WriteCommand>) parameters[i++];
      pendingPrepares = (List<PrepareCommand>) parameters[i++];
   }

   @Override
   public String toString() {
      return "RehashControlCommand{" +
            "type=" + type +
            ", sender=" + sender +
            ", state=" + state +
            ", consistentHash=" + consistentHash +
            ", txLogCommands=" + txLogCommands +
            ", pendingPrepares=" + pendingPrepares +
            '}';
   }
}

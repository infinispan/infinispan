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
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.NodeTopologyInfo;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

/**
 * A control command to coordinate rehashes that may occur when nodes join or leave a cluster, when DIST is used as a
 * cache mode.  This complex command coordinates the various phases of a rehash event when a joiner joins or a leaver
 * leaves a cluster running in "distribution" mode.
 * <p />
 * It may break up into several commands in future.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = ReplicableCommandExternalizer.class, id = Ids.REHASH_CONTROL_COMMAND)
public class RehashControlCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 17;

   public enum Type {
      JOIN_REQ, JOIN_REHASH_START, JOIN_REHASH_END, JOIN_ABORT, PULL_STATE_JOIN, PULL_STATE_LEAVE, PUSH_STATE, DRAIN_TX, DRAIN_TX_PREPARES
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
         case JOIN_REQ:
            return distributionManager.requestPermissionToJoin(sender);
         case JOIN_REHASH_START:
            return distributionManager.informRehashOnJoin(sender, true, nodeTopologyInfo);
         case JOIN_REHASH_END:
            distributionManager.informRehashOnJoin(sender, false, nodeTopologyInfo);
            return null;
         case PULL_STATE_JOIN:
            return pullStateForJoin();             
         case PULL_STATE_LEAVE:
             return pullStateForLeave();    
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

   public Map<Object, InternalCacheValue> pullStateForJoin() throws CacheLoaderException {           
      
      Map<Object, InternalCacheValue> state = new HashMap<Object, InternalCacheValue>();
      for (InternalCacheEntry ice : dataContainer) {
         Object k = ice.getKey();
         if (shouldTransferOwnershipToJoinNode(k)) {            
             state.put(k, ice.toInternalCacheValue());
         }
      }

      CacheStore cacheStore = distributionManager.getCacheStoreForRehashing();
      if (cacheStore != null) {
         for (Object k: cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer))) {
            if (!state.containsKey(k) && shouldTransferOwnershipToJoinNode(k)) {                
               InternalCacheValue v = loadValue(cacheStore, k);               
               if (v != null) state.put(k, v);
            }
         }
      }
      return state;
   }
   
   public Map<Object, InternalCacheValue> pullStateForLeave() throws CacheLoaderException {
     
      Map<Object, InternalCacheValue> state = new HashMap<Object, InternalCacheValue>();
      for (InternalCacheEntry ice : dataContainer) {
         Object k = ice.getKey();
         if (shouldTransferOwnershipFromLeftNodes(k)) {
            state.put(k, ice.toInternalCacheValue());
         }
      }

      CacheStore cacheStore = distributionManager.getCacheStoreForRehashing();
      if (cacheStore != null) {
         for (Object k : cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer))) {
            if (!state.containsKey(k) && shouldTransferOwnershipFromLeftNodes(k)) {
               InternalCacheValue v = loadValue(cacheStore, k);               
               if (v != null)
                  state.put(k, v);
            }
         }
      }
      return state;
   }
   
   private boolean shouldTransferOwnershipFromLeftNodes(Object k) {      
      Address self = transport.getAddress();      
      int numCopies = configuration.getNumOwners();
      
      List<Address> oldList = oldCH.locate(k, numCopies);
      boolean localToThisNode = oldList.indexOf(self) >= 0;
      boolean senderIsNewOwner = newCH.isKeyLocalToAddress(sender, k, numCopies);
      for (Address leftNodeAddress : nodesLeft) {
         boolean localToLeftNode = oldList.indexOf(leftNodeAddress) >= 0;
         if (localToLeftNode && senderIsNewOwner && localToThisNode) {
            return true;
         }
      }
      return false;
   }
      

   private InternalCacheValue loadValue(CacheStore cs, Object k) {
      try {
         InternalCacheEntry ice = cs.load(k);
         return ice == null ? null : ice.toInternalCacheValue();
      } catch (CacheLoaderException cle) {
         log.warn("Unable to load " + k + " from cache loader", cle);
      }
      return null;
   }

   final boolean shouldTransferOwnershipToJoinNode(Object k) {     
      Address self = transport.getAddress();      
      int numCopies = configuration.getNumOwners(); 
      List<Address> oldOwnerList = oldCH.locate(k, numCopies);
      if (!oldOwnerList.isEmpty() && self.equals(oldOwnerList.get(0))) {
         List<Address> newOwnerList = newCH.locate(k, numCopies);
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
            ", state=" + state +
            ", oldConsistentHash=" + oldCH +
            ", nodesLeft=" + nodesLeft +
            ", consistentHash=" + newCH +
            ", txLogCommands=" + txLogCommands +
            ", pendingPrepares=" + pendingPrepares +
            '}';
   }
}

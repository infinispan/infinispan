package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This abstraction performs RPCs and works on a TransactionLogger located on a remote node.
 *
 * @author Manik Surtani
 * @since 4.2.1
 */
public class RemoteTransactionLoggerImpl implements RemoteTransactionLogger {
   private final CommandsFactory commandsFactory;
   private final Address targetNode, sender;
   private final RpcManager rpcManager;
   private boolean drainWithoutLock = true;
   private Collection<PrepareCommand> pendingPrepares;

   public RemoteTransactionLoggerImpl(CommandsFactory commandsFactory, Address targetNode, RpcManager rpcManager) {
      this.commandsFactory = commandsFactory;
      this.targetNode = targetNode;
      this.rpcManager = rpcManager;
      sender = rpcManager.getAddress();
   }

   private RemoteTransactionLogDetails extractRemoteTransactionLogDetails(ReplicableCommand c) {
      Map<Address, Response> lr = rpcManager.invokeRemotely(Collections.singleton(targetNode), c, true, true);
      if (lr.size() != 1) throw new RpcException("Expected just one response; got " + lr + " instead!");
      Response r = lr.get(targetNode);
      if (r != null && r.isSuccessful() && r.isValid()) {
         return (RemoteTransactionLogDetails) ((SuccessfulResponse) r).getResponseValue();
      } else {
         throw new RpcException("Invalid response " + r);
      }
   }

   @Override
   public List<WriteCommand> drain() {
      ReplicableCommand c = commandsFactory.buildRehashControlCommand(RehashControlCommand.Type.JOIN_TX_LOG_REQ, sender);
      RemoteTransactionLogDetails details = extractRemoteTransactionLogDetails(c);
      drainWithoutLock = details.isDrainNextCallWithoutLock();
      return details.getModifications();
   }

   @Override
   public List<WriteCommand> drainAndLock(Address notUsed) {
      ReplicableCommand c = commandsFactory.buildRehashControlCommand(RehashControlCommand.Type.JOIN_TX_FINAL_LOG_REQ, sender);
      RemoteTransactionLogDetails details = extractRemoteTransactionLogDetails(c);
      pendingPrepares = details.getPendingPreparesMap();
      return details.getModifications();
   }

   @Override
   public boolean shouldDrainWithoutLock() {
      return drainWithoutLock;
   }

   @Override
   public Collection<PrepareCommand> getPendingPrepares() {
      return pendingPrepares;
   }

   @Override
   public void unlockAndDisable(Address notUsed) {
      ReplicableCommand c = commandsFactory.buildRehashControlCommand(RehashControlCommand.Type.JOIN_TX_LOG_CLOSE, sender);
      rpcManager.invokeRemotely(Collections.singleton(targetNode), c, true, true);
   }
}

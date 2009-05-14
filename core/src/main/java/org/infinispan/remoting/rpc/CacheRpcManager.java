package org.infinispan.remoting.rpc;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.ReplicationException;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;

/**
 * Class that encapsulates the logic for replicating commands through cluster participants.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class CacheRpcManager {

   private static Log log = LogFactory.getLog(CacheRpcManager.class);
   private static final boolean trace = log.isTraceEnabled();

   private Configuration configuration;
   private boolean stateTransferEnabled;

   private ReplicationQueue replicationQueue;
   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;


   @Inject
   public void init(ReplicationQueue replicationQueue, RpcManager rpcManager, CommandsFactory commandsFactory, Configuration configuration) {
      this.replicationQueue = replicationQueue;
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.configuration = configuration;
   }

   @Start
   public void init() {
      stateTransferEnabled = configuration.isFetchInMemoryState();
   }

   public void broadcastRpcCommand(CacheRpcCommand rpc, boolean sync, boolean useOutOfBandMessage) throws ReplicationException {
      if (useReplicationQueue(sync)) {
         replicationQueue.add(rpc);
      } else {
         multicastRpcCommand(null, rpc, sync, useOutOfBandMessage);
      }
   }

   public void broadcastReplicableCommand(ReplicableCommand call, boolean sync) throws ReplicationException {
      multicastReplicableCommand(null, call, sync);
   }

   public void multicastReplicableCommand(List<Address> members, ReplicableCommand call, boolean sync) throws ReplicationException {
      if (useReplicationQueue(sync)) {
         replicationQueue.add(call);
      } else {
         SingleRpcCommand rpcCommand = commandsFactory.buildSingleRpcCommand(call);
         multicastRpcCommand(members, rpcCommand, sync, false);
      }
   }

   private ResponseMode getResponseMode(boolean sync) {
      return sync ? ResponseMode.SYNCHRONOUS : configuration.isUseAsyncMarshalling() ? ResponseMode.ASYNCHRONOUS : ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING;
   }

   public void multicastRpcCommand(List<Address> recipients, CacheRpcCommand command, boolean sync, boolean useOutOfBandMessage) throws ReplicationException {
      if (trace) {
         log.trace("invoking method " + command.getClass().getSimpleName() + ", members=" + rpcManager.getTransport().getMembers() + ", mode=" +
               configuration.getCacheMode() + ", exclude_self=" + true + ", timeout=" +
               configuration.getSyncReplTimeout());
         log.trace("Broadcasting call " + command + " to recipient list " + recipients);
      }

      List rsps;
      try {
         rsps = rpcManager.invokeRemotely(recipients,
                                          command,
                                          getResponseMode(sync),
                                          configuration.getSyncReplTimeout(), useOutOfBandMessage, stateTransferEnabled
         );
         if (trace) log.trace("responses=" + rsps);
         if (sync) checkResponses(rsps);
      } catch (CacheException e) {
         log.error("Replication exception", e);
         throw e;
      } catch (Exception ex) {
         log.error("Unexpected exception", ex);
         throw new ReplicationException("Unexpected exception while replicating", ex);
      }
   }

   public boolean isClusterStarted() {
      return rpcManager != null && rpcManager.getTransport() != null && rpcManager.getTransport().getMembers() != null
            && rpcManager.getTransport().getMembers().size() > 1;
   }

   private boolean useReplicationQueue(boolean sync) {
      return !sync && replicationQueue != null;
   }

   /**
    * Checks whether any of the responses are exceptions. If yes, re-throws them (as exceptions or runtime exceptions).
    */
   private void checkResponses(List rsps) {
      if (rsps != null) {
         for (Object rsp : rsps) {
            if (rsp != null && rsp instanceof Throwable) {
               // lets print a stack trace first.
               Throwable throwable = (Throwable) rsp;
               if (trace) {
                  log.trace("Received Throwable from remote cache", throwable);
               }
               throw new ReplicationException(throwable);
            }
         }
      }
   }

   public Address getLocalAddress() {
      return rpcManager != null ? rpcManager.getLocalAddress() : null;
   }
}

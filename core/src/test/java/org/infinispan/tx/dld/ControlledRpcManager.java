package org.infinispan.tx.dld;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.remoting.ReplicationException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferException;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
* @author Mircea.Markus@jboss.com
* @since 4.2
*/
public class ControlledRpcManager implements RpcManager {

   private static Log log = LogFactory.getLog(ControlledRpcManager.class);

   private volatile CountDownLatch replicationLatch;

   private boolean fail;

   public ControlledRpcManager(RpcManager realOne) {
      this.realOne = realOne;
   }

   protected RpcManager realOne;

   public boolean isFail() {
      return fail;
   }

   public void setFail(boolean fail) {
      this.fail = fail;
   }

   public void setReplicationLatch(CountDownLatch replicationLatch) {
      this.replicationLatch = replicationLatch;
   }

   public List<Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
      waitFirst(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter);
   }

   public List<Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
      waitFirst(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
   }

   public List<Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) throws Exception {
      waitFirst(rpcCommand);
      return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout);
   }

   public void retrieveState(String cacheName, long timeout) throws StateTransferException {
      failIfNeeded();
      realOne.retrieveState(cacheName, timeout);
   }

   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws ReplicationException {
      waitFirst(rpc);
      realOne.broadcastRpcCommand(rpc, sync);
   }

   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws ReplicationException {
      waitFirst(rpc);
      realOne.broadcastRpcCommand(rpc, sync, usePriorityQueue);
   }

   protected void waitFirst(ReplicableCommand rpcCommand) {
      failIfNeeded();
      boolean isLockControlCommand = rpcCommand instanceof LockControlCommand;
      boolean isClusterGet = rpcCommand instanceof ClusteredGetCommand;
      if (!isClusterGet && !isLockControlCommand) {
         log.info(Thread.currentThread().getName() + " -- replication trigger called!");
         waitForLatchToOpen();
      }
   }

   protected void waitForLatchToOpen() {
      try {
         replicationLatch.await();
         log.trace("Replication latch opened, continuing...");
      } catch (Exception e) {
         throw new RuntimeException("Unexpected exception!", e);
      }
   }

   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      waitFirst(rpc);
      realOne.broadcastRpcCommandInFuture(rpc, future);
   }

   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      waitFirst(rpc);
      realOne.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
   }

   public void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws ReplicationException {
      waitFirst(rpc);
      realOne.invokeRemotely(recipients, rpc, sync);
   }

   public List<Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws ReplicationException {
      List<Response> responses = realOne.invokeRemotely(recipients, rpc, sync, usePriorityQueue);
      waitForLatchToOpen();
      return responses;
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      waitFirst(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, future);
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
      waitFirst(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
   }

   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout) {
      waitFirst(rpc);
      realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
   }

   public Transport getTransport() {
      return realOne.getTransport();
   }

   public Address getCurrentStateTransferSource() {
      return realOne.getCurrentStateTransferSource();
   }

   public Address getAddress() {
      return realOne.getAddress();
   }

   public void failIfNeeded() {
      if (fail) throw new IllegalStateException("Induced failure!");
   }
}

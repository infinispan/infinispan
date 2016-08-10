package org.infinispan.statetransfer;

import java.util.concurrent.CompletableFuture;

public class DelegatingStateTransferLock implements StateTransferLock {
   private final StateTransferLock delegate;

   public DelegatingStateTransferLock(StateTransferLock delegate) {
      this.delegate = delegate;
   }

   @Override
   public void acquireExclusiveTopologyLock() {
      delegate.acquireExclusiveTopologyLock();
   }

   @Override
   public void releaseExclusiveTopologyLock() {
      delegate.releaseExclusiveTopologyLock();
   }

   @Override
   public void acquireSharedTopologyLock() {
      delegate.acquireSharedTopologyLock();
   }

   @Override
   public void releaseSharedTopologyLock() {
      delegate.releaseSharedTopologyLock();
   }

   @Override
   public void notifyTransactionDataReceived(int topologyId) {
      delegate.notifyTransactionDataReceived(topologyId);
   }

   @Override
   public CompletableFuture<Void> transactionDataFuture(int expectedTopologyId) {
      return delegate.transactionDataFuture(expectedTopologyId);
   }

   @Override
   public boolean transactionDataReceived(int expectedTopologyId) {
      return delegate.transactionDataReceived(expectedTopologyId);
   }

   @Override
   public CompletableFuture<Void> topologyFuture(int expectedTopologyId) {
      return delegate.topologyFuture(expectedTopologyId);
   }

   @Override
   public void notifyTopologyInstalled(int topologyId) {
      delegate.notifyTopologyInstalled(topologyId);
   }

   @Override
   public boolean topologyReceived(int expectedTopologyId) {
      return delegate.topologyReceived(expectedTopologyId);
   }
}

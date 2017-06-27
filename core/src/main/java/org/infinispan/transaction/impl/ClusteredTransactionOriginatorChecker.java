package org.infinispan.transaction.impl;

import java.util.Collection;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * A {@link TransactionOriginatorChecker} implementation for clustered caches.
 * <p>
 * It uses the current topology to fetch the live members to check if the transaction's originator is alive.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class ClusteredTransactionOriginatorChecker implements TransactionOriginatorChecker {

   private RpcManager rpcManager;

   @Inject
   public void injectRpcManager(RpcManager rpcManager) {
      this.rpcManager = rpcManager;
   }

   @Override
   public boolean isOriginatorMissing(GlobalTransaction gtx) {
      return isOriginatorMissing(gtx, rpcManager.getMembers());
   }

   @Override
   public boolean isOriginatorMissing(GlobalTransaction gtx, Collection<Address> liveMembers) {
      return !liveMembers.contains(gtx.getAddress());
   }
}

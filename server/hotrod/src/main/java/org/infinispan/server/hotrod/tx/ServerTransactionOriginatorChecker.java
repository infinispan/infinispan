package org.infinispan.server.hotrod.tx;

import java.util.Collection;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.TransactionOriginatorChecker;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * A {@link TransactionOriginatorChecker} implementation that is aware of client transactions.
 * <p>
 * The transaction originator in this case is the Hot Rod client.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class ServerTransactionOriginatorChecker implements TransactionOriginatorChecker {

   private RpcManager rpcManager;

   public ServerTransactionOriginatorChecker() {
   }

   @Inject
   public void inject(RpcManager rpcManager) {
      this.rpcManager = rpcManager;
   }

   @Override
   public boolean isOriginatorMissing(GlobalTransaction gtx) {
      return isOriginatorMissing(gtx, rpcManager.getMembers());
   }

   @Override
   public boolean isOriginatorMissing(GlobalTransaction gtx, Collection<Address> liveMembers) {
      return !liveMembers.contains(gtx.getAddress()) && isNonClientTransaction(gtx);
   }

   private boolean isNonClientTransaction(GlobalTransaction gtx) {
      return !(gtx.getAddress() instanceof ClientAddress);
   }
}

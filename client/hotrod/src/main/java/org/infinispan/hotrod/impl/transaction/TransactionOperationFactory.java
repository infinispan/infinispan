package org.infinispan.hotrod.impl.transaction;

import javax.transaction.xa.Xid;

import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transaction.operations.CompleteTransactionOperation;
import org.infinispan.hotrod.impl.transaction.operations.ForgetTransactionOperation;
import org.infinispan.hotrod.impl.transaction.operations.RecoveryOperation;
import org.infinispan.hotrod.impl.transport.netty.ChannelFactory;

/**
 * An operation factory that builds operations independent from the cache used.
 * <p>
 * This operations are the commit/rollback request, forget request and in-doubt transactions request.
 * <p>
 * This operation aren't associated to any cache, but they use the default cache topology to pick the server to
 * contact.
 *
 * @since 14.0
 */
public class TransactionOperationFactory {
   private final OperationContext operationContext;

   public TransactionOperationFactory(HotRodConfiguration configuration, ChannelFactory channelFactory, Codec codec) {
      this.operationContext = new OperationContext(channelFactory, codec, null, configuration, null, null, null);
   }

   CompleteTransactionOperation newCompleteTransactionOperation(Xid xid, boolean commit) {
      return new CompleteTransactionOperation(operationContext, xid, commit);
   }

   ForgetTransactionOperation newForgetTransactionOperation(Xid xid) {
      return new ForgetTransactionOperation(operationContext, xid);
   }

   RecoveryOperation newRecoveryOperation() {
      return new RecoveryOperation(operationContext);
   }

}

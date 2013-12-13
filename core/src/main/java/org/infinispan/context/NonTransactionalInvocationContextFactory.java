package org.infinispan.context;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.RemoteTransaction;

import javax.transaction.Transaction;

/**
 * Invocation Context container to be used for non-transactional caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
@SurvivesRestarts
public class NonTransactionalInvocationContextFactory extends AbstractInvocationContextFactory {

   @Inject
   public void init(Configuration config) {
      super.init(config);
   }

   @Override
   public InvocationContext createInvocationContext(boolean isWrite, int keyCount) {
      if (keyCount == 1) {
         return new SingleKeyNonTxInvocationContext(true, keyEq);
      } else if (keyCount > 0) {
         return new NonTxInvocationContext(keyCount, true, keyEq);
      }
      return createInvocationContext(null);
   }

   @Override
   public InvocationContext createInvocationContext(Transaction tx) {
      return createNonTxInvocationContext();
   }

   @Override
   public NonTxInvocationContext createNonTxInvocationContext() {
      NonTxInvocationContext ctx = new NonTxInvocationContext(keyEq);
      ctx.setOriginLocal(true);
      return ctx;
   }

   @Override
   public InvocationContext createSingleKeyNonTxInvocationContext() {
      return new SingleKeyNonTxInvocationContext(true, keyEq);
   }

   @Override
   public NonTxInvocationContext createRemoteInvocationContext(Address origin) {
      NonTxInvocationContext ctx = new NonTxInvocationContext(keyEq);
      ctx.setOrigin(origin);
      return ctx;
   }

   @Override
   public LocalTxInvocationContext createTxInvocationContext() {
      throw exception();
   }

   @Override
   public RemoteTxInvocationContext createRemoteTxInvocationContext(
         RemoteTransaction tx, Address origin) {
      throw exception();
   }

   private IllegalStateException exception() {
      return new IllegalStateException("This is a non-transactional cache - why need to build a transactional context for it!");
   }
}

package org.infinispan.context;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;

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
         return new SingleKeyNonTxInvocationContext(null, keyEq);
      } else if (keyCount > 0) {
         return new NonTxInvocationContext(keyCount, null, keyEq);
      }
      return createInvocationContext(null, false);
   }

   @Override
   public InvocationContext createInvocationContext(Transaction tx, boolean implicitTransaction) {
      return createNonTxInvocationContext();
   }

   @Override
   public NonTxInvocationContext createNonTxInvocationContext() {
      NonTxInvocationContext ctx = new NonTxInvocationContext(null, keyEq);
      return ctx;
   }

   @Override
   public InvocationContext createSingleKeyNonTxInvocationContext() {
      return new SingleKeyNonTxInvocationContext(null, keyEq);
   }

   @Override
   public NonTxInvocationContext createRemoteInvocationContext(Address origin) {
      NonTxInvocationContext ctx = new NonTxInvocationContext(origin, keyEq);
      return ctx;
   }

   @Override
   public LocalTxInvocationContext createTxInvocationContext(LocalTransaction localTransaction) {
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

   @Override
   public InvocationContext createRemoteInvocationContextForCommand(VisitableCommand cacheCommand,
                                                                          Address origin) {
      if (cacheCommand instanceof DataCommand && !(cacheCommand instanceof InvalidateCommand)) {
         return new SingleKeyNonTxInvocationContext(origin, keyEq);
      } else {
         return super.createRemoteInvocationContextForCommand(cacheCommand, origin);
      }
   }
}

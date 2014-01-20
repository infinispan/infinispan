package org.infinispan.context;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;

import javax.transaction.Transaction;

/**
 * Factory for {@link InvocationContext} objects.
 *
 * @author Manik Surtani (manik AT infinispan DOT org)
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 7.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface InvocationContextFactory {
   /**
    * To be used when building InvocationContext with {@link #createInvocationContext(boolean, int)} as an indicator
    * of the fact that the size of the keys to be accessed in the context is not known.
    */
   int UNBOUNDED = -1;

   /**
    * If we are in a tx scope this will return an {@link org.infinispan.context.impl.TxInvocationContext}. Otherwise it
    * will return an {@link org.infinispan.context.impl.NonTxInvocationContext}. Either way, both context will be marked
    * as local, i.e. {@link org.infinispan.context.InvocationContext#isOriginLocal()} will be true.
    */
   InvocationContext createInvocationContext(boolean isWrite, int keyCount);

   /**
    * Creates an invocation context
    *
    * @param tx
    * @return
    */
   InvocationContext createInvocationContext(Transaction tx, boolean implicitTransaction);

   /**
    * Will create an {@link org.infinispan.context.impl.NonTxInvocationContext} with the {@link
    * org.infinispan.context.impl.NonTxInvocationContext#isOriginLocal()} returning true.
    */
   NonTxInvocationContext createNonTxInvocationContext();

   /**
    * Will create an {@link org.infinispan.context.impl.NonTxInvocationContext} with the {@link
    * org.infinispan.context.impl.NonTxInvocationContext#isOriginLocal()} returning true.
    */
   InvocationContext createSingleKeyNonTxInvocationContext();

   /**
    * Returns a {@link org.infinispan.context.impl.LocalTxInvocationContext}.
    */
   LocalTxInvocationContext createTxInvocationContext(LocalTransaction localTransaction);

   /**
    * Returns an {@link org.infinispan.context.impl.RemoteTxInvocationContext}.
    *
    * @param tx remote transaction
    * @param origin the origin of the command, or null if local
    */
   RemoteTxInvocationContext createRemoteTxInvocationContext(RemoteTransaction tx, Address origin);

   /**
    * Returns an {@link org.infinispan.context.impl.NonTxInvocationContext} whose {@link
    * org.infinispan.context.impl.NonTxInvocationContext#isOriginLocal()} flag will be true.
    *
    * @param origin the origin of the command, or null if local
    */
   InvocationContext createRemoteInvocationContext(Address origin);

   /**
    * As {@link #createRemoteInvocationContext(org.infinispan.remoting.transport.Address)},
    * but returning the flags to the context from the Command if any Flag was set.
    *
    * @param cacheCommand
    * @param origin       the origin of the command, or null if local
    */
   InvocationContext createRemoteInvocationContextForCommand(VisitableCommand cacheCommand, Address origin);
}

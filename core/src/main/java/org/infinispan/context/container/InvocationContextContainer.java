package org.infinispan.context.container;

import org.infinispan.context.impl.InitiatorTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * // TODO: Mircea: Document this!
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@NonVolatile
@Scope(Scopes.NAMED_CACHE)
public interface InvocationContextContainer {
   InvocationContext getLocalInvocationContext(boolean prepareForCall);

   InitiatorTxInvocationContext getInitiatorTxInvocationContext();

   RemoteTxInvocationContext getRemoteTxInvocationContext(GlobalTransaction globalTransaction, boolean create);

   InvocationContext getRemoteNonTxInvocationContext();

   InvocationContext getThreadContext();

   Object suspend();

   void resume(Object backup);
}

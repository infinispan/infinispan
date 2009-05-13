package org.infinispan.context;

import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

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
   InvocationContext getLocalInvocationContext();

   LocalTxInvocationContext getInitiatorTxInvocationContext();

   RemoteTxInvocationContext getRemoteTxInvocationContext();

   InvocationContext getRemoteNonTxInvocationContext();

   InvocationContext getThreadContext();

   Object suspend();

   void resume(Object backup);
}
